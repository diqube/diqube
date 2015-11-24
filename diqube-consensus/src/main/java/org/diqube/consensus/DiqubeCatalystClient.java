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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.TransportException;

/**
 *
 * @author Bastian Gloeckle
 */
public class DiqubeCatalystClient implements Client {

  private DiqubeCatalystConnectionFactory factory;

  private List<DiqubeCatalystConnection> connections = new ArrayList<>();

  public DiqubeCatalystClient(DiqubeCatalystConnectionFactory factory) {
    this.factory = factory;
  }

  @Override
  public CompletableFuture<Connection> connect(Address address) {
    CompletableFuture<Connection> res = new CompletableFuture<>();

    try {
      DiqubeCatalystConnection con = factory.createDiqubeCatalystConnection();
      con.openAndRegister(address);
      connections.add(con);
      res.complete(con);
    } catch (TransportException e) {
      res.completeExceptionally(e);
    }

    return res;
  }

  @Override
  public CompletableFuture<Void> close() {
    connections.forEach(con -> con.close());
    CompletableFuture<Void> res = new CompletableFuture<>();
    res.complete(null);
    return res;
  }

}
