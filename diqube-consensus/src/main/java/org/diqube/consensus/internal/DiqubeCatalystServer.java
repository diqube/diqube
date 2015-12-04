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
package org.diqube.consensus.internal;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import org.diqube.context.AutoInstatiate;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.util.concurrent.ThreadContext;

/**
 * The catalyst server which is used internally by copycat.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCatalystServer implements Server {

  private Consumer<Connection> listener;
  private Deque<DiqubeCatalystConnection> connections = new ConcurrentLinkedDeque<>();
  private ThreadContext context;
  private boolean initialized = false;
  private CompletableFuture<Void> listenFuture = null;
  private boolean doAllowCompletionOfListen = false;

  @Override
  public CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
    this.listener = listener;
    context = ThreadContext.currentContextOrThrow();
    initialized = true;
    synchronized (this) {
      listenFuture = new CompletableFuture<>();
      if (doAllowCompletionOfListen) {
        context.executor().execute(() -> listenFuture.complete(null));
        listenFuture = null;
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  public void allowCompletionOfListen() {
    synchronized (this) {
      doAllowCompletionOfListen = true;
      if (listenFuture != null)
        context.executor().execute(() -> listenFuture.complete(null));
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    while (!connections.isEmpty())
      connections.poll().close();

    CompletableFuture<Void> res = new CompletableFuture<>();
    res.complete(null);
    return res;
  }

  public void newClientConnection(DiqubeCatalystConnection con) {
    if (!initialized)
      // should never happen as long as copycat does the right thing.
      throw new IllegalStateException("Not initialized.");

    connections.add(con);
    // execute listeners synchronously, as the connection will be initialized by those listeners.
    CompletableFuture.runAsync(() -> listener.accept(con), context.executor()).join();
  }

}
