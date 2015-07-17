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

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.thrift.TServiceClient;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.NodeAddress;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool for thrift connections based on {@link RNodeAddress}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConnectionPool {
  static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

  @Config(ConfigKey.CLIENT_SOCKET_TIMEOUT_MS)
  private int socketTimeout;

  private ConnectionFactory connectionFactory;

  @Inject
  private ClusterManager clusterManager;

  @PostConstruct
  public void initialize() {
    connectionFactory = new DefaultConnectionFactory(this, socketTimeout);
  }

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
   * @param socketListener
   *          A listener that will be informed if anything happens to the connection. Can be <code>null</code>. The
   *          remote node the connection was opened to will be removed from {@link ClusterManager} automatically before
   *          calling this listener.
   * @throws ConnectionException
   *           if connection could not be opened. The corresponding remote will be automatically removed from
   *           {@link ClusterManager}.
   */
  public <T extends TServiceClient> Connection<T> reserveConnection(Class<T> thiftClientClass, String thriftServiceName,
      RNodeAddress addr, SocketListener socketListener) throws ConnectionException {
    // TODO #32 implement pooling.
    // TODO #32 add thread that sends keep-alives. If one fails, inform all currently reserved connections to that
    // remote.

    try {
      return connectionFactory.createConnection(thiftClientClass, thriftServiceName, addr, new SocketListener() {
        @Override
        public void connectionDied() {
          // TODO #32: retry if connection was pooled.
          logger.warn("Connection to {} died unexpectedly.", new NodeAddress(addr));
          clusterManager.nodeDied(addr);

          if (socketListener != null)
            socketListener.connectionDied();
        }
      });
    } catch (ConnectionException e) {
      logger.warn("Could not connect to {}.", new NodeAddress(addr));
      clusterManager.nodeDied(addr);
      throw new ConnectionException("Error connecting to " + addr, e);
    }
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

  /**
   * Override connection factory for tests.
   */
  /* package */ void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public static class ConnectionException extends RuntimeException {
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
