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
package org.diqube.connection;

import org.diqube.remote.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.remote.base.thrift.RNodeAddress;

/**
 * Factory for new connections to other machines.
 *
 * @author Bastian Gloeckle
 */
public interface ConnectionFactory {
  /**
   * Create and open a new connection.
   * 
   * @param serviceInfo
   *          The service info of the thrift service to open a connection for.
   * @param addr
   *          The address to open a connection to.
   * @param socketListener
   *          Listener on events from the socket.
   * @return The new connection.
   * @throws ConnectionException
   *           If anything happens.
   */
  public <T> Connection<T> createConnection(DiqubeThriftServiceInfo<T> serviceInfo, RNodeAddress addr,
      SocketListener socketListener) throws ConnectionException;

  /**
   * Create a new connection object based on an old one, whose socket is still open. The new one will provide a
   * different service than the old one.
   * 
   * All properties of the old connection will be copied to the new one.
   * 
   * Be aware that this does not change the {@link SocketListener}!
   * 
   * @param oldConnection
   *          The {@link Connection} whose socket should be used. Do not use this connection object any more after
   *          calling this method! The connection will therefore be disabled and a subsequent call to
   *          {@link Connection#getService()} will throw an exception.
   * @param serviceInfo
   *          The service info of the thrift service to open a connection for.
   * @return The new connection.
   * @throws ConnectionException
   *           if anything went wrong.
   */
  public <T, U> Connection<U> createConnection(Connection<T> oldConnection, DiqubeThriftServiceInfo<U> serviceInfo)
      throws ConnectionException;
}