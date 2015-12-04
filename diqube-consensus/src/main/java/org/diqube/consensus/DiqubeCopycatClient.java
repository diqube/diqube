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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.consensus.internal.DiqubeCatalystSerializer;
import org.diqube.consensus.internal.DiqubeCatalystTransport;
import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.DiqubeConsensusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.session.Session;
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

  private ReentrantReadWriteLock newClientOpensLock = new ReentrantReadWriteLock();
  private Deque<CompletableFuture<RaftClient>> waitingClients = new ConcurrentLinkedDeque<>();

  private volatile CoycatClientFacade client;

  private volatile boolean consensusIsInitialized = false;

  public RaftClient getClient() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = new CoycatClientFacade(() -> CopycatClient.builder(copycatServer.getClusterMembers())
              .withTransport(transport).withSerializer(serializer)
              // connect to any server.
              .withConnectionStrategy(ConnectionStrategies.ANY) //
              .withRecoveryStrategy(RecoveryStrategies.RECOVER).build());
        }
      }
    }
    return client;
  }

  /**
   * Creates and returns an object implementing the given stateMachineInterface which will, when methods are called,
   * distribute those calls among the consensus cluster and only return when the {@link Command}/{@link Query} is
   * committed in the cluster.
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
  public <T> T getStateMachineClient(Class<T> stateMachineInterface) {
    Map<String, Class<? extends Operation<?>>> operationClassesByMethodName =
        stateMachineManager.getOperationClassesAndMethodNamesOfInterface(stateMachineInterface);

    if (operationClassesByMethodName.isEmpty())
      // no operations in interface, probably no ConsensusStateMachine annotated interface!
      return null;

    RaftClient client = getClient();

    InvocationHandler h = new InvocationHandler() {
      @SuppressWarnings("unchecked")
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (!operationClassesByMethodName.containsKey(method.getName()))
          throw new RuntimeException("Unknown method: " + method.getName());

        if (args.length != 1 || !(args[0] instanceof Commit))
          throw new RuntimeException("Invalid parameters!");

        Commit<?> c = (Commit<?>) args[0];

        while (true) {
          try {
            logger.trace("Opening copycat client (consensusIsInitialized = {})...", consensusIsInitialized);
            client.open().join();
            logger.trace("Copycat client opened.");
            return client.submit(c.operation()).join();
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

          logger.info("Retrying to open copycat client/submit something to consensus cluster.");
        }
      }
    };

    @SuppressWarnings("unchecked")
    T resProxy =
        (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { stateMachineInterface }, h);

    return resProxy;
  }

  @Override
  public void consensusInitialized() {
    newClientOpensLock.writeLock().lock();
    try {
      consensusIsInitialized = true;

      waitingClients.forEach(completableFuture -> completableFuture.complete(null));
      waitingClients = null;
    } finally {
      newClientOpensLock.writeLock().unlock();
    }
  }

  @PreDestroy
  public void cleanup() {
    if (client != null) {
      logger.trace("Cleaning up copycat client...");
      client.close();
      client = null;
    }
  }

  /**
   * Facade on {@link RaftClient} that slows down {@link #open()} calls until the local consensus server is initialized.
   */
  private class CoycatClientFacade implements RaftClient {
    private volatile CopycatClient delegate;
    private Supplier<CopycatClient> delegateSupplier;

    /* package */ CoycatClientFacade(Supplier<CopycatClient> delegateSupplier) {
      this.delegateSupplier = delegateSupplier;
    }

    @Override
    public ThreadContext context() {
      return delegate.context();
    }

    @Override
    public Transport transport() {
      return delegate.transport();
    }

    @Override
    public Serializer serializer() {
      return delegate.serializer();
    }

    @Override
    public Session session() {
      return delegate.session();
    }

    @Override
    public <T> CompletableFuture<T> submit(Command<T> command) {
      return delegate.submit(command);
    }

    @Override
    public <T> CompletableFuture<T> submit(Query<T> query) {
      return delegate.submit(query);
    }

    @Override
    public CompletableFuture<RaftClient> open() {
      if (consensusIsInitialized && delegate != null) {
        logger.trace("Calling delegate to open consensus client...");
        return delegate.open();
      }

      newClientOpensLock.readLock().lock();
      try {
        if (!consensusIsInitialized) {
          CompletableFuture<RaftClient> res = new CompletableFuture<>();
          waitingClients.add(res);
          return res.thenCompose(v -> {
            if (delegate == null) {
              synchronized (delegateSupplier) {
                if (delegate == null)
                  delegate = delegateSupplier.get();
              }
            }
            return delegate.open();
          });
        } else if (delegate == null) {
          synchronized (delegateSupplier) {
            if (delegate == null)
              delegate = delegateSupplier.get();
          }
          logger.trace("Calling delegate to open consensus client...");
          return delegate.open();
        } else {
          logger.trace("Calling delegate to open consensus client...");
          return delegate.open();
        }
      } finally {
        newClientOpensLock.readLock().unlock();
      }
    }

    @Override
    public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override
    public CompletableFuture<Void> close() {
      return delegate.close();
    }

    @Override
    public boolean isClosed() {
      return delegate.isClosed();
    }
  }
}
