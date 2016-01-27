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

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.transport.TTransport;
import org.diqube.thrift.base.services.DiqubeThriftServiceInfoManager.DiqubeThriftServiceInfo;
import org.diqube.thrift.base.thrift.RNodeAddress;

/**
 * A connection to a server.
 * 
 * This is {@link Closeable}: When closed, {@link ConnectionPool#releaseConnection(Connection)} will be called.
 *
 * @author Bastian Gloeckle
 */
public class Connection<T> implements Closeable, ServiceProvider<T> {
  private T service;
  private TTransport transport;
  private RNodeAddress address;
  private ConnectionPool parentPool;
  private UUID executionUuid = null;
  private boolean wasReplaced = false;
  private AtomicBoolean isPooled = new AtomicBoolean(true);
  private AtomicLong timeout = null;
  private DiqubeThriftServiceInfo<T> serviceInfo;

  /* package */ Connection(ConnectionPool parentPool, DiqubeThriftServiceInfo<T> serviceInfo, T service,
      TTransport transport, RNodeAddress address) {
    this.parentPool = parentPool;
    this.serviceInfo = serviceInfo;
    this.service = service;
    this.transport = transport;
    this.address = address;
  }

  /**
   * @return Easy to use service bean - each method call on the returned object will actually trigger a remote call.
   * @throws IllegalStateException
   *           if connection was replaced or connection is pooled.
   */
  @Override
  public T getService() throws IllegalStateException {
    if (wasReplaced)
      throw new IllegalStateException("Connection disabled (replaced): " + System.identityHashCode(getTransport()) + " "
          + System.identityHashCode(this));
    if (isPooled.get())
      throw new IllegalStateException(
          "Connection not reserved: " + System.identityHashCode(getTransport()) + " " + System.identityHashCode(this));
    return service;
  }

  /* package */TTransport getTransport() {
    return transport;
  }

  /* package */ RNodeAddress getAddress() {
    return address;
  }

  /* package */ DiqubeThriftServiceInfo<T> getServiceInfo() {
    return serviceInfo;
  }

  /**
   * @return The executionUuid this connection was working for. Can be <code>null</code>.
   */
  /* package */UUID getExecutionUuid() {
    return executionUuid;
  }

  /**
   * @return <code>true</code> if the connection can be used, <code>false</code> if this connection cannot be used any
   *         more because it was used as "oldConnection" in a call to
   *         {@link ConnectionFactory#createConnection(Connection, Class, String)} as "old connection".
   */
  public boolean wasReplaced() {
    return wasReplaced;
  }

  /* package */ void setWasReplaced(boolean wasReplaced) {
    this.wasReplaced = wasReplaced;
  }

  /**
   * @return <code>true</code> if this connection is currently available in the ConnectionPool, <code>false</code> if it
   *         is reserved by someone. Default value is "true".
   */
  public boolean isPooled() {
    return isPooled.get();
  }

  /**
   * comapre-and-set "pooled" value. Returns <code>true</code> if successful.
   */
  /* package */ boolean pooledCAS(boolean expected, boolean newValue) {
    return this.isPooled.compareAndSet(expected, newValue);
  }

  /**
   * For {@link ConnectionPool} only: Set the executionUuid this connection is working either when this connection is
   * being reserved or is being released.
   */
  /* package */void setExecutionUuid(UUID executionUuid) {
    this.executionUuid = executionUuid;
  }

  /* package */ Long getTimeout() {
    return (timeout == null) ? null : timeout.get();
  }

  /* package */ void setTimeout(Long timeout) {
    if (timeout == null)
      this.timeout = null;

    if (this.timeout == null) {
      this.timeout = new AtomicLong(timeout);
      return;
    }

    this.timeout.set(timeout);
  }

  @Override
  public void close() throws IOException {
    parentPool.releaseConnection(this);
  }

  @Override
  public boolean isLocal() {
    return false;
  }

}