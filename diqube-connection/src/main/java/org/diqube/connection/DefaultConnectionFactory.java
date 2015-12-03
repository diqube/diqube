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

import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.connection.integrity.IntegrityCheckingProtocol;
import org.diqube.connection.integrity.IntegritySecretHelper;
import org.diqube.remote.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.thriftutil.RememberingTransport;

/**
 * Default implementation for {@link ConnectionFactory}.
 *
 * @author Bastian Gloeckle
 */
/* package */class DefaultConnectionFactory implements ConnectionFactory {
  private final ConnectionPool connectionPool;
  private int socketTimeout;
  private byte[][] macKeys;

  /**
   * @param connectionPool
   *          The pool this factory belongs to.
   */
  /* package */ DefaultConnectionFactory(ConnectionPool connectionPool, IntegritySecretHelper integritySecretHelper,
      int socketTimeout) {
    this.connectionPool = connectionPool;
    this.socketTimeout = socketTimeout;

    macKeys = integritySecretHelper.provideMessageIntegritySecrets();
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
  public <T> Connection<T> createConnection(DiqubeThriftServiceInfo<T> serviceInfo, RNodeAddress addr,
      SocketListener socketListener) throws ConnectionException {

    TTransport transport = openTransport(addr, socketListener);

    T queryResultClient = createProtocolAndClient(serviceInfo, transport);
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new ConnectionException("Could not open connection to " + addr, e);
    }

    return new Connection<>(connectionPool, serviceInfo, queryResultClient, transport, addr);
  }

  @Override
  public <T, U> Connection<U> createConnection(Connection<T> oldConnection, DiqubeThriftServiceInfo<U> serviceInfo)
      throws ConnectionException {
    U client = createProtocolAndClient(serviceInfo, oldConnection.getTransport());

    oldConnection.setEnabled(false);

    Connection<U> res =
        new Connection<>(connectionPool, serviceInfo, client, oldConnection.getTransport(), oldConnection.getAddress());
    res.setTimeout(oldConnection.getTimeout());
    res.setExecutionUuid(oldConnection.getExecutionUuid());
    return res;
  }

  private <T> T createProtocolAndClient(DiqubeThriftServiceInfo<T> serviceInfo, TTransport transport)
      throws ConnectionException {

    TProtocol innerProtocol;
    if (serviceInfo.isIntegrityChecked()) {
      if (!(transport instanceof RememberingTransport))
        transport = new RememberingTransport(transport);
      innerProtocol = new IntegrityCheckingProtocol(new TCompactProtocol(transport), macKeys);
    } else
      innerProtocol = new TCompactProtocol(transport);

    TProtocol queryProtocol = new TMultiplexedProtocol(innerProtocol, serviceInfo.getServiceName());

    try {
      @SuppressWarnings("unchecked")
      T queryResultClient = (T) serviceInfo.getClientClass().getConstructor(TProtocol.class).newInstance(queryProtocol);
      return queryResultClient;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      ConnectionPool.logger.error("Error while constructing a client", e);
      throw new ConnectionException("Error while constructing a client", e);
    }
  }
}