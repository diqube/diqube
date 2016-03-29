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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.inject.Inject;

import org.diqube.consensus.internal.DiqubeCatalystSerializer;
import org.diqube.consensus.internal.DiqubeCatalystTransport;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.listeners.ConsensusListener;
import org.diqube.util.Holder;
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

  private Supplier<CopycatClient> copycatClientFactory = () -> CopycatClient
      .builder(consensusServer.getClusterMembers()).withTransport(transport).withSerializer(serializer) //
      .withConnectionStrategy(ConnectionStrategies.EXPONENTIAL_BACKOFF) //
      .withServerSelectionStrategy(ServerSelectionStrategies.LEADER) //
      .withRetryStrategy(RetryStrategies.RETRY) // we will retry
      .withRecoveryStrategy(RecoveryStrategies.RECOVER) // recover from expired sessions
      .build();

  private Holder<CopycatClient> currentCopycatClientHolder = new Holder<>();

  private AtomicBoolean wasShutdown = new AtomicBoolean(false);

  @Override
  public <T> ClosableProvider<T> getStateMachineClient(Class<T> stateMachineInterface)
      throws ConsensusClusterUnavailableException {
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

        Commit<?> c = (Commit<?>) args[0];
        synchronized (currentCopycatClientHolder) {
          if (wasShutdown.get()) {
            logger.error("Cannot submit commit {} since consensus client was shutdown already. Ignoring.", c);
            throw new IllegalStateException("Consensus client closed already.");
          }

          if (currentCopycatClientHolder.getValue() == null) {
            CopycatClient newClient = copycatClientFactory.get();
            logger.trace("Opening new copycat client...");
            newClient.connect().join();
            currentCopycatClientHolder.setValue(newClient);
          }

          logger.trace("Submitting consensus request...");
          CopycatClient client = currentCopycatClientHolder.getValue();
          return client.submit(c.operation()).join();
        }
      }
    };

    @SuppressWarnings("unchecked")
    T proxy = (T) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { stateMachineInterface }, h);

    if (!consensusServer.clusterSeemsFunctional()) {
      logger.error("Consensus cluster seems to not be available. Is there an ongoing network partition?");
      throw new ConsensusClusterUnavailableException(
          "Consensus cluster seems to not be available. Is there an ongoing network partition?");
    }

    return new ClosableProvider<T>() {
      @Override
      public void close() {
        // noop. TODO perhaps close currently opened client if noone else is using it.
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

  @Override
  public void contextAboutToShutdown() {
    synchronized (currentCopycatClientHolder) {
      if (currentCopycatClientHolder.getValue() != null) {
        logger.info("Closing copycat client...");
        currentCopycatClientHolder.getValue().close().join();
        currentCopycatClientHolder.setValue(null);
        logger.info("Copycat client closed.");
        wasShutdown.set(true);
      }
    }
  }

}
