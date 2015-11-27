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

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.inject.Inject;

import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.DiqubeConsensusListener;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.session.Session;

/**
 * Provides a {@link RaftClient} which can be used to interact with the consensus cluster.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCopycatClient implements DiqubeConsensusListener {
  @Inject
  private DiqubeCatalystTransport transport;

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  @Inject
  private DiqubeCatalystSerializer serializer;

  private ReentrantReadWriteLock newClientOpensLock = new ReentrantReadWriteLock();
  private Deque<CompletableFuture<RaftClient>> waitingClients = new ConcurrentLinkedDeque<>();

  private volatile CoycatClientFacade client;

  private boolean consensusIsInitialized = false;

  public RaftClient getClient() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          NodeAddress ourAddr = ourNodeAddressProvider.getOurNodeAddress();

          Address catalystAddr = new Address(ourAddr.getHost(), ourAddr.getPort());

          client = new CoycatClientFacade(
              CopycatClient.builder(catalystAddr).withTransport(transport).withSerializer(serializer)
                  // connect only to local server.
                  .withConnectionStrategy((leader, members) -> Arrays.asList(catalystAddr))
                  .withRecoveryStrategy(RecoveryStrategies.RECOVER).build());
        }
      }
    }
    return client;
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

  /**
   * Facade on {@link RaftClient} that slows down {@link #open()} calls until the local consensus server is initialized.
   */
  private class CoycatClientFacade implements RaftClient {
    private CopycatClient delegate;

    /* package */ CoycatClientFacade(CopycatClient delegate) {
      this.delegate = delegate;
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
      if (consensusIsInitialized)
        return delegate.open();

      newClientOpensLock.readLock().lock();
      try {
        if (!consensusIsInitialized) {
          CompletableFuture<RaftClient> res = new CompletableFuture<>();
          res.handle((result, error) -> delegate.open());
          waitingClients.add(res);
          return res;
        } else
          return delegate.open();
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
