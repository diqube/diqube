/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.consensus;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.consensus.internal.DiqubeCatalystSerializer;
import org.diqube.consensus.internal.DiqubeCatalystTransport;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.listeners.ConsensusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;

import io.atomix.copycat.Operation;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.RetryStrategies;
import io.atomix.copycat.client.ServerSelectionStrategies;
import io.atomix.copycat.server.Commit;

/**
 * Default implementation for {@link ConsensusClient}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.CONSENSUS)
public class DefaultConsensusClient implements ConsensusListener, ConsensusClient {
  private static final Logger logger = LoggerFactory.getLogger(ConsensusClient.class);

  @Inject
  private DiqubeCatalystTransport transport;

  @Inject
  private DiqubeCatalystSerializer serializer;

  @Inject
  private ConsensusStateMachineManager stateMachineManager;

  @Inject
  private ConsensusServer consensusServer;

  private ReentrantReadWriteLock consensusInitializedWaitingLock = new ReentrantReadWriteLock();
  private Deque<CompletableFuture<Void>> consensusWaitingFutures = new ConcurrentLinkedDeque<>();

  private volatile boolean consensusIsInitialized = false;

  /**
   * The {@link copycatClientProvider} which is capable of recreating the client.
   * 
   * <p>
   * When recreating the client, always use the most up-to-date list of cluster members from {@link #consensusServer}.
   */
  private CopycatClientProvider copycatClientProvider = new CopycatClientProvider(() -> CopycatClient
      .builder(consensusServer.getClusterMembers()).withTransport(transport).withSerializer(serializer) //
      .withConnectionStrategy(ConnectionStrategies.EXPONENTIAL_BACKOFF) //
      .withServerSelectionStrategy(ServerSelectionStrategies.ANY) //
      .withRetryStrategy(RetryStrategies.RETRY) //
      .withRecoveryStrategy(RecoveryStrategies.RECOVER).build());

  @Override
  public <T> ClosableProvider<T> getStateMachineClient(Class<T> stateMachineInterface) throws IllegalStateException {
    // only execute this after consensus server was initialized fully!
    waitUntilConsensusServerIsInitialized();

    Map<String, Class<? extends Operation<?>>> operationClassesByMethodName =
        stateMachineManager.getOperationClassesAndMethodNamesOfInterface(stateMachineInterface);

    if (operationClassesByMethodName.isEmpty())
      // no operations in interface, probably no ConsensusStateMachine annotated interface!
      return null;

    InvocationHandler h = new InvocationHandler() {
      @SuppressWarnings("unchecked")
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!operationClassesByMethodName.containsKey(method.getName()))
          throw new RuntimeException("Unknown method: " + method.getName());

        if (args.length != 1 || !(args[0] instanceof Commit))
          throw new RuntimeException("Invalid parameters!");

        CopycatClient copycatClient = copycatClientProvider.getClient();
        Commit<?> c = (Commit<?>) args[0];

        while (true) {
          try {
            logger.trace("Opening copycat client...");
            copycatClient.open().join();
            logger.trace("Copycat client opened, submitting request...");
            return copycatClient.submit(c.operation()).join();
          } catch (CompletionException e) {
            // This can happen if our node currently has no connection to the consensus leader, as new client sessions
            // are always registered with the leader.
            logger.error("Could not open copycat client/submit something to consensus cluster! Will retry shortly.", e);
          }
          ThreadLocalRandom random = ThreadLocalRandom.current();
          long targetSleepMs = consensusServer.getElectionTimeoutMs() / 3;
          long deltaMs = targetSleepMs / 10;

          // sleep random time, from 10% below "target" to 10% above "target".
          try {
            long waitTime = random.nextLong(targetSleepMs - deltaMs, targetSleepMs + deltaMs);
            logger.trace("Waiting {} ms...", waitTime);
            Thread.sleep(waitTime);
          } catch (InterruptedException e) {
            throw new ConsensusStateMachineClientInterruptedException("Interrupted while waiting", e);
          }

          // In case we did not succeed, we fully re-create the CopycatClient. Copycat seems to open a connection to a
          // random node, but may then, on an error stick to that node, if the connection was opened successfully. This
          // was seen for example when a connection to a PASSIVE node was opened - connection can be opened just fine,
          // but the RegisterRequests of the new ClientSession are not accepted by PassiveState and Errors are returned.
          // The CopycatClient then might not choose another node to connect to, but always the same one, leaving us
          // with no successful ClientSession. We therefore clean all state the client might have by simply recreating
          // it fully (and initializing it with the currently active nodes in the consensus cluster which are retrieved
          // from ConsensusServer).
          // It could have been that copycat did not connect to new servers, because diqube had a buggy implementation
          // of catyst client/connection. But even then,, it is meaningful to recreate the client once and then, as the
          // nodes that are in the cluster might change and we'd like to initialize the client session freshly.

          logger.trace("Restarting consensus client...");
          copycatClientProvider.close(); // unregister
          logger.trace("Restarting consensus client (2)...");
          copycatClientProvider.recreateClient().join(); // recreate, wait until recreated.
          logger.trace("Restarting consensus client (3)...");
          copycatClientProvider.registerUsage(); // register again (if another thread started recreation, this will
                                                 // block!)
          logger.trace("Restarting consensus client (4)...");
          copycatClient = copycatClientProvider.getClient(); // get new client.
          logger.trace("Restarting consensus client done.");
        }
      }
    };

    @SuppressWarnings("unchecked")
    T proxy = (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { stateMachineInterface }, h);

    if (!consensusServer.clusterSeemsFunctional()) {
      logger.error("Consensus cluster seems to not be available. Is there an ongoing network partition?");
      throw new IllegalStateException(
          "Consensus cluster seems to not be available. Is there an ongoing network partition?");
    }

    // initial registration, will unregister on #close!
    copycatClientProvider.registerUsage();
    return new ClosableProvider<T>() {
      @Override
      public void close() {
        copycatClientProvider.close();
      }

      @Override
      public T getClient() {
        return proxy;
      }
    };
  }

  @Override
  public void consensusInitialized() {
    consensusInitializedWaitingLock.writeLock().lock();
    try {
      consensusIsInitialized = true;

      consensusWaitingFutures.forEach(completableFuture -> completableFuture.complete(null));
      consensusWaitingFutures = null;
    } finally {
      consensusInitializedWaitingLock.writeLock().unlock();
    }
  }

  private void waitUntilConsensusServerIsInitialized() {
    if (!consensusIsInitialized) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      consensusInitializedWaitingLock.readLock().lock();
      try {
        if (!consensusIsInitialized)
          consensusWaitingFutures.add(f);
        else
          f.complete(null);
      } finally {
        consensusInitializedWaitingLock.readLock().unlock();
      }
      f.join();
    }
  }

  @PreDestroy
  public void cleanup() {
    copycatClientProvider.shutdown();
  }

  /**
   * Provider of a {@link CopycatClient} which tracks the number of times the current client is used (see
   * {@link #registerUsage()} and {@link #close()}) and which has the capability to re-create the client completely (=
   * close the old one and create a new one that will need to be {@link CopycatClient#open()}ed again). This is
   * meaningful for retrying connections that are broken.
   */
  private class CopycatClientProvider implements ClosableProvider<CopycatClient> {

    /** Number of times the CopycatClient is in use by users currently. */
    private AtomicInteger useCount = new AtomicInteger(0);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * If != null then someone requested to reopen the CopycatClient and this future will be completed, once the new
     * client is created.
     */
    private volatile CompletableFuture<CopycatClient> newClientFuture;

    private Supplier<CopycatClient> clientFactory;
    private volatile CopycatClient currentClient;

    /* package */ CopycatClientProvider(Supplier<CopycatClient> clientFactory) {
      this.clientFactory = clientFactory;
    }

    /**
     * Request for the CopycatClient to be recreated. The new client will then need to be re-opened fully.
     * 
     * @return A {@link CompletableFuture} that will be completed as soon as the new client is available. The returned
     *         future will never be completed exceptionally.
     */
    /* package */ CompletableFuture<CopycatClient> recreateClient() {
      CompletableFuture<CopycatClient> f = newClientFuture;
      if (f != null)
        return f;

      lock.writeLock().lock();
      try {
        if (newClientFuture != null)
          return newClientFuture;

        newClientFuture = new CompletableFuture<>();

        CompletableFuture<CopycatClient> newFuture = newClientFuture;

        if (useCount.get() == 0) {
          // trigger execution of opening new client.
          registerUsage();
          close();
        }

        return newFuture;
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public void close() {
      int c = useCount.decrementAndGet();
      if (c == 0 && newClientFuture != null) {
        lock.writeLock().lock();
        // double checked locking
        try {
          if (useCount.get() == 0 && newClientFuture != null) {
            logger.debug("(Re)creating consensus client...");
            // close old client and create a new one.
            try {
              if (currentClient != null)
                currentClient.close().join();
            } catch (CompletionException e) {
              logger.warn("Could not close old consensus client", e);
              // swallow otherwise, there's nothing we can do...
            }

            currentClient = clientFactory.get();

            logger.debug("Consensus client (re)created.");
            newClientFuture.complete(currentClient);
            newClientFuture = null;
          }
        } finally {
          lock.writeLock().unlock();
        }
      }
    }

    /**
     * The currently valid {@link CopycatClient}. Call only after calling {@link #registerUsage()} and before calling
     * {@link #close()}!
     */
    @Override
    public CopycatClient getClient() {
      // save without locking, because this is only called if useCount > 0

      CopycatClient res = currentClient;

      // lazily create client, as on instantiation we cannot create it, since consensus server is not ready and the
      // factory needs that!
      while (res == null) {
        res = currentClient;
        if (res == null) {
          CompletableFuture<CopycatClient> c = newClientFuture;
          close();
          if (c == null)
            c = recreateClient();
          c.join();
          registerUsage();
          res = currentClient;
        }
      }

      return res;
    }

    /**
     * Register one more user of the CopycatClient.
     */
    /* package */void registerUsage() {
      lock.readLock().lock();
      try {
        useCount.incrementAndGet();
      } finally {
        lock.readLock().unlock();
      }
    }

    /* package */ void shutdown() {
      lock.writeLock().lock();
      try {
        logger.trace("Cleaning up consensus client...");
        currentClient.close().join();
        currentClient = null;
      } finally {
        lock.writeLock().unlock();
      }
    }

  }
}
