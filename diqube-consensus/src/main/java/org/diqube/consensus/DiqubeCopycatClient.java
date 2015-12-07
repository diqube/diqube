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
import org.diqube.listeners.DiqubeConsensusListener;
import org.diqube.util.CloseableNoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.server.Commit;

/**
 * Provides a {@link RaftClient} which can be used to interact with the consensus cluster.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCopycatClient implements DiqubeConsensusListener {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeCopycatClient.class);

  @Inject
  private DiqubeCatalystTransport transport;

  @Inject
  private DiqubeCatalystSerializer serializer;

  @Inject
  private DiqubeConsensusStateMachineManager stateMachineManager;

  @Inject
  private DiqubeCopycatServer copycatServer;

  private ReentrantReadWriteLock consensusInitializedWaitingLock = new ReentrantReadWriteLock();
  private Deque<CompletableFuture<Void>> consensusWaitingFutures = new ConcurrentLinkedDeque<>();

  private volatile boolean consensusIsInitialized = false;

  /**
   * The {@link RaftClientProvider} which is capable of recreating the client.
   * 
   * <p>
   * When recreating the client, always use the most up-to-date list of cluster members from {@link #copycatServer}.
   */
  private RaftClientProvider raftClientProvider = new RaftClientProvider(
      () -> CopycatClient.builder(copycatServer.getClusterMembers()).withTransport(transport).withSerializer(serializer)
          // connect to any server.
          .withConnectionStrategy(ConnectionStrategies.ANY) //
          .withRecoveryStrategy(RecoveryStrategies.RECOVER).build());

  /**
   * Creates and returns an object implementing the given stateMachineInterface which will, when methods are called,
   * distribute those calls among the consensus cluster and only return when the {@link Command}/{@link Query} is
   * committed in the cluster.
   * 
   * <p>
   * The returned {@link ClosableProvider} can provide the created instance and must be closed after the client is not
   * needed any more!
   * 
   * <p>
   * Note that methods on the returned object are always executed synchronously and it may take an arbitrary time too
   * complete (e.g. if there is no consensus leader currently and a client session cannot be opened therefore). This is
   * especially true for nodes which are partitioned on the network from the majority of the cluster: When a method on
   * the returned object is called in such a case, it may take up until the network partition is resolved and the
   * cluster became fully available again until the methods return!
   * 
   * <p>
   * The returned object might throw {@link DiqubeConsensusStateMachineClientInterruptedException} on each method call,
   * since pure {@link InterruptedException}s cannot be thrown through the proxy. Be sure to catch them and throw the
   * encapsulated {@link InterruptedException}.
   * 
   * @param stateMachineInterface
   *          Interface which has the {@link ConsensusStateMachine} annotation.
   */
  public <T> ClosableProvider<T> getStateMachineClient(Class<T> stateMachineInterface) {
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

        RaftClient raftClient = raftClientProvider.getClient();
        Commit<?> c = (Commit<?>) args[0];

        while (true) {
          try {
            logger.trace("Opening copycat client...");
            raftClient.open().join();
            logger.trace("Copycat client opened, submitting request...");
            return raftClient.submit(c.operation()).join();
          } catch (CompletionException e) {
            // This can happen if our node currently has no connection to the consensus leader, as new client sessions
            // are always registered with the leader.
            logger.error("Could not open copycat client/submit something to consensus cluster! Will retry shortly.", e);
          }
          ThreadLocalRandom random = ThreadLocalRandom.current();
          long targetSleepMs = copycatServer.getElectionTimeoutMs() / 3;
          long deltaMs = targetSleepMs / 10;

          // sleep random time, from 10% below "target" to 10% above "target".
          try {
            Thread.sleep(random.nextLong(targetSleepMs - deltaMs, targetSleepMs + deltaMs));
          } catch (InterruptedException e) {
            throw new DiqubeConsensusStateMachineClientInterruptedException("Interrupted while waiting", e);
          }

          // In case we did not succeed, we fully re-create the RaftClient. Copycat seems to open a connection to a
          // random node, but may then, on an error stick to that node, if the connection was opened successfully. This
          // was seen for example when a connection to a PASSIVE node was opened - connection can be opened just fine,
          // but the RegisterRequests of the new ClientSession are not accepted by PassiveState and Errors are returned.
          // The CopycatClient then might not choose another node to connect to, but always the same one, leaving us
          // with no successful ClientSession. We therefore clean all state the client might have by simply recreating
          // it fully (and initializing it with the currently active nodes in the consensus cluster which are retrieved
          // from DiqubeCopycatServer).
          // It could have been that copycat did not connect to new servers, because diqube had a buggy implementation
          // of catyst client/connection. But even then,, it is meaningful to recreate the client once and then, as the
          // nodes that are in the cluster might change and we'd like to initialize the client session freshly.

          raftClientProvider.close(); // unregister
          raftClientProvider.recreateClient().join(); // recreate, wait until recreated.
          raftClientProvider.registerUsage(); // register again (if another thread started recreation, this will block!)
          raftClient = raftClientProvider.getClient(); // get new client.
        }
      }
    };

    @SuppressWarnings("unchecked")
    T proxy = (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { stateMachineInterface }, h);

    // initial registration, will unregister on #close!
    raftClientProvider.registerUsage();
    return new ClosableProvider<T>() {
      @Override
      public void close() {
        raftClientProvider.close();
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
    raftClientProvider.shutdown();
  }

  /**
   * Provider of a {@link RaftClient} which tracks the number of times the current client is used (see
   * {@link #registerUsage()} and {@link #close()}) and which has the capability to re-create the client completely (=
   * close the old one and create a new one that will need to be {@link RaftClient#open()}ed again). This is meaningful
   * for retrying connections that are broken.
   */
  private class RaftClientProvider implements ClosableProvider<RaftClient> {

    /** Number of times the RaftClient is in use by users currently. */
    private AtomicInteger useCount = new AtomicInteger(0);

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * If != null then someone requested to reopen the RaftClient and this future will be completed, once the new client
     * is created.
     */
    private volatile CompletableFuture<Void> newClientFuture;

    private Supplier<RaftClient> clientFactory;
    private volatile RaftClient currentClient;

    /* package */ RaftClientProvider(Supplier<RaftClient> clientFactory) {
      this.clientFactory = clientFactory;
    }

    /**
     * Request for the RaftClient to be recreated. The new client will then need to be re-opened fully.
     * 
     * @return A {@link CompletableFuture} that will be completed as soon as the new client is available. The returned
     *         future will never be completed exceptionally.
     */
    /* package */ CompletableFuture<Void> recreateClient() {
      CompletableFuture<Void> f = newClientFuture;
      if (f != null)
        return f;

      lock.writeLock().lock();
      try {
        if (newClientFuture != null)
          return newClientFuture;

        newClientFuture = new CompletableFuture<>();

        CompletableFuture<Void> newFuture = newClientFuture;

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
            logger.debug("Recreating consensus client...");
            // close old client and create a new one.
            try {
              currentClient.close().join();
            } catch (CompletionException e) {
              logger.warn("Could not close old consensus client", e);
              // swallow otherwise, there's nothing we can do...
            }

            currentClient = clientFactory.get();

            logger.debug("Consensus client recreated.");
            newClientFuture.complete(null);
            newClientFuture = null;
          }
        } finally {
          lock.writeLock().unlock();
        }
      }
    }

    /**
     * The currently valid {@link RaftClient}. Call only after calling {@link #registerUsage()} and before calling
     * {@link #close()}!
     */
    @Override
    public RaftClient getClient() {
      // save without locking, because this is only called if useCount > 0

      // lazily create client, as on instantiation we cannot create it, since consensus server is not ready and the
      // factory needs that!
      if (currentClient == null) {
        synchronized (this) {
          if (currentClient == null)
            currentClient = clientFactory.get();
        }
      }

      return currentClient;
    }

    /**
     * Register one more user of the RaftClient.
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
        logger.trace("Cleaning up copycat client...");
        currentClient.close().join();
        currentClient = null;
      } finally {
        lock.writeLock().unlock();
      }
    }

  }

  /**
   * Provides a client to a consensus client. Needs to be {@link #close()}d correctly!
   */
  public static interface ClosableProvider<T> extends CloseableNoException {
    public T getClient();
  }
}
