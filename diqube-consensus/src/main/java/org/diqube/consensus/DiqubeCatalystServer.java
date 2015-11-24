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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.DiqubeConsensusListener;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;

/**
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCatalystServer implements Server, DiqubeConsensusListener {

  private Consumer<Connection> listener;
  private List<DiqubeCatalystConnection> connections = new ArrayList<>();
  private boolean consensusInitialized = false;
  private ReentrantReadWriteLock initializeLock = new ReentrantReadWriteLock();

  @Override
  public CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
    this.listener = listener;
    CompletableFuture<Void> res = new CompletableFuture<>();
    res.complete(null);
    return res;
  }

  @Override
  public CompletableFuture<Void> close() {
    connections.forEach(con -> con.close());

    CompletableFuture<Void> res = new CompletableFuture<>();
    res.complete(null);
    return res;
  }

  public void newClientConnection(DiqubeCatalystConnection con) {
    initializeLock.readLock().lock();
    try {
      connections.add(con);
      if (consensusInitialized)
        listener.accept(con);
    } finally {
      initializeLock.readLock().unlock();
    }
  }

  @Override
  public void consensusInitialized() {
    initializeLock.writeLock().lock();
    try {
      consensusInitialized = true;
      if (!connections.isEmpty())
        connections.forEach(con -> listener.accept(con));
    } finally {
      initializeLock.writeLock().unlock();
    }
  }

}
