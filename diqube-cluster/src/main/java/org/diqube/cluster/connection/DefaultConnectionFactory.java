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
package org.diqube.cluster.connection;

import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.remote.base.thrift.RNodeAddress;

/**
 * Default implementation for {@link ConnectionFactory}.
 *
 * @author Bastian Gloeckle
 */
class DefaultConnectionFactory implements ConnectionFactory {
  private final ConnectionPool connectionPool;
  private int socketTimeout;

  /**
   * @param connectionPool
   *          The pool this factory belongs to.
   */
  /* package */ DefaultConnectionFactory(ConnectionPool connectionPool, int socketTimeout) {
    this.connectionPool = connectionPool;
    this.socketTimeout = socketTimeout;
  }

  private TTransport openTransport(RNodeAddress addr, SocketListener socketListener) throws ConnectionException {
    if (addr.isSetHttpAddr()) {
      try {
        // TODO #32: Integrate SocketListener into HTTP connections.
        return new THttpClient(addr.getHttpAddr().getUrl());
      } catch (TTransportException e) {
        throw new ConnectionException("Could not open connection to " + addr, e);
      }
    }

    TTransport transport = new DiqubeClientSocket(addr.getDefaultAddr().getHost(), addr.getDefaultAddr().getPort(),
        socketTimeout, socketListener);
    return new TFramedTransport(transport);
  }

  @Override
  public <T extends TServiceClient> Connection<T> createConnection(Class<T> thriftClientClass, String thriftServiceName,
      RNodeAddress addr, SocketListener socketListener) throws ConnectionException {

    TTransport transport = openTransport(addr, socketListener);

    T queryResultClient = createProtocolAndClient(thriftClientClass, thriftServiceName, transport);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new ConnectionException("Could not open connection to " + addr, e);
    }

    return new Connection<>(connectionPool, thriftClientClass, queryResultClient, transport, addr);
  }

  private <T extends TServiceClient> T createProtocolAndClient(Class<T> thriftClientClass, String thriftServiceName,
      TTransport transport) throws ConnectionException {
    TProtocol queryProtocol = new TMultiplexedProtocol(new TCompactProtocol(transport), thriftServiceName);

    T queryResultClient;
    try {
      queryResultClient = thriftClientClass.getConstructor(TProtocol.class).newInstance(queryProtocol);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      ConnectionPool.logger.error("Error while constructing a client", e);
      throw new ConnectionException("Error while constructing a client", e);
    }
    return queryResultClient;
  }

  @Override
  public <T extends TServiceClient, U extends TServiceClient> Connection<U> createConnection(
      Connection<T> oldConnection, Class<U> newThriftClientClass, String newThriftServiceName)
          throws ConnectionException {
    U client = createProtocolAndClient(newThriftClientClass, newThriftServiceName, oldConnection.getTransport());

    return new Connection<>(connectionPool, newThriftClientClass, client, oldConnection.getTransport(),
        oldConnection.getAddress());
  }

}