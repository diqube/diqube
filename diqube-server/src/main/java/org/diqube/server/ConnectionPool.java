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
package org.diqube.server;

import java.lang.reflect.InvocationTargetException;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.query.QueryResultServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool for thrift connections based on {@link RNodeAddress}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConnectionPool {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

  /**
   * Reserve a connection for the given remote addrress.
   * 
   * <p>
   * Please be aware that the returned object is NOT thread-safe and may be used by one thread simultaniously only.
   * 
   * <p>
   * For each object reserved by this method, {@link #releaseConnection(RNodeAddress, Object)} needs to be called!
   * 
   * @param thiftClientClass
   *          The class of the thrift service client that the caller wants to access.
   * @param addr
   *          The address of the node to open a connection to.
   */
  public <T extends TServiceClient> Connection<T> reserveConnection(Class<T> thiftClientClass, RNodeAddress addr)
      throws ConnectionException {
    // TODO implement pooling.
    // TODO implement failsafe connections!
    // TODO implement timeout

    TTransport transport = openTransport(addr);

    TProtocol queryProtocol =
        new TMultiplexedProtocol(new TCompactProtocol(transport), QueryResultServiceConstants.SERVICE_NAME);

    T queryResultClient;
    try {
      queryResultClient = thiftClientClass.getConstructor(TProtocol.class).newInstance(queryProtocol);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      logger.error("Error while constructing a client", e);
      throw new ConnectionException("Error while constructing a client", e);
    }
    try {
      transport.open();
    } catch (TTransportException e) {
      throw new ConnectionException("Could not open connection to " + addr, e);
    }

    return new Connection<>(queryResultClient, transport, addr);
  }

  private TTransport openTransport(RNodeAddress addr) throws ConnectionException {
    if (addr.isSetHttpAddr()) {
      try {
        return new THttpClient(addr.getHttpAddr().getUrl());
      } catch (TTransportException e) {
        throw new ConnectionException("Could not open connection to " + addr, e);
      }
    }

    TTransport transport = new TSocket(addr.getDefaultAddr().getHost(), addr.getDefaultAddr().getPort());
    return new TFramedTransport(transport);
  }

  /**
   * Release a connection that was reserved using {@link #reserveConnection(Class, RNodeAddress)}.
   * 
   * @param connection
   *          The connection returned by {@link #reserveConnection(Class, RNodeAddress)}.
   */
  public <T> void releaseConnection(Connection<T> connection) {
    connection.getTransport().close();
  }

  public static class Connection<T> {
    private T service;
    private TTransport transport;
    private RNodeAddress address;

    private Connection(T service, TTransport transport, RNodeAddress address) {
      this.service = service;
      this.transport = transport;
      this.address = address;

    }

    public T getService() {
      return service;
    }

    private TTransport getTransport() {
      return transport;
    }

    private RNodeAddress getAddress() {
      return address;
    }
  }

  public class ConnectionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ConnectionException() {
      super();
    }

    public ConnectionException(String msg) {
      super(msg);
    }

    public ConnectionException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
