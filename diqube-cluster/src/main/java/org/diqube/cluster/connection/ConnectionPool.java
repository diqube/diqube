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

import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.NodeAddress;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool for thrift connections based on {@link RNodeAddress}.
 * 
 * This class internally works in a way that it holds open connections to remotes which are not currently reserved.
 * These connections are all fitted with {@link DefaultSocketListener}s, in order that this class can manage the
 * behaviour of these listeners. If the connection gets reserved, the {@link DefaultSocketListener} will be set into a
 * mode where it forwards events to the custom specified {@link SocketListener}. When it is not reserved, no events are
 * fowarded.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ConnectionPool implements ClusterManagerListener {
  static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

  @Config(ConfigKey.CLIENT_SOCKET_TIMEOUT_MS)
  private int socketTimeout;

  @Config(ConfigKey.KEEP_ALIVE_MS)
  private int keepAlive;

  private KeepAliveThread keepAliveThread = new KeepAliveThread();

  private ConnectionFactory connectionFactory;

  @Inject
  private ClusterManager clusterManager;

  /**
   * Connections by remote address which are currently not used. Each connection is fitted with a
   * {@link DefaultSocketListener} which can be fetched using {@link #defaultSocketListeners}.
   * 
   * If a connection dies while being reserved (= not being inside this map), just do not return the connection into
   * this map. Remember to remove resources of dead connections in {@link #defaultSocketListeners}.
   */
  private Map<RNodeAddress, Deque<Connection<? extends TServiceClient>>> availableConnections =
      new ConcurrentHashMap<>();

  /**
   * The {@link DefaultSocketListener} that a specific connection is bound to.
   */
  private Map<Connection<? extends TServiceClient>, DefaultSocketListener> defaultSocketListeners =
      new ConcurrentHashMap<>();

  @PostConstruct
  public void initialize() {
    connectionFactory = new DefaultConnectionFactory(this, socketTimeout);
    keepAliveThread.start();
  }

  @PreDestroy
  public void cleanup() {
    keepAliveThread.interrupt();
  }

  /**
   * Reserve a connection for the given remote address.
   * 
   * <p>
   * Please be aware that the returned object is NOT thread-safe and may be used by one thread simultaneously only.
   * 
   * <p>
   * For each object reserved by this method, {@link #releaseConnection(RNodeAddress, Object)} needs to be called!
   * 
   * @param thriftClientClass
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
  @SuppressWarnings("unchecked")
  public <T extends TServiceClient> Connection<T> reserveConnection(Class<T> thriftClientClass,
      String thriftServiceName, RNodeAddress addr, SocketListener socketListener) throws ConnectionException {

    // TODO #32: Add max number of connections to one node
    // TODO #32: Close connections after they have not been used for some time.

    Deque<Connection<? extends TServiceClient>> conns = availableConnections.get(addr);

    if (conns != null) {
      while (!conns.isEmpty()) {
        Connection<? extends TServiceClient> conn = conns.poll();
        if (conn != null) {
          // we reserved "conn".

          // first we fetch a connection to a ClusterManagementService to execute a "ping".
          Connection<KeepAliveService.Client> keepAliveConn;
          if (conn.getService() instanceof KeepAliveService.Iface)
            keepAliveConn = (Connection<KeepAliveService.Client>) conn;
          else {
            keepAliveConn = connectionFactory.createConnection(conn, KeepAliveService.Client.class,
                KeepAliveServiceConstants.SERVICE_NAME);

            defaultSocketListeners.put(keepAliveConn, defaultSocketListeners.get(conn));
            defaultSocketListeners.remove(conn);
          }

          // try to ping connection, see if it is still alive.
          try {
            keepAliveConn.getService().ping();

            // yes, is still alive, so use it! Make sure it is a connection of the requested service.
            Connection<T> res = connectionFactory.createConnection(keepAliveConn, thriftClientClass, thriftServiceName);
            defaultSocketListeners.put(res, defaultSocketListeners.get(keepAliveConn));
            defaultSocketListeners.remove(keepAliveConn);
            defaultSocketListeners.get(res).init(socketListener);
            return res;
          } catch (TException e) {
            // swallow. connection is not alive any more. try next one (or open a new one).
            defaultSocketListeners.remove(conn);
          }
        }
      }
    }

    // create a new connection.
    try {
      DefaultSocketListener newDefaultSocketListener = new DefaultSocketListener(addr);
      Connection<T> res =
          connectionFactory.createConnection(thriftClientClass, thriftServiceName, addr, newDefaultSocketListener);
      newDefaultSocketListener.init(socketListener);
      newDefaultSocketListener.setParentConnection(res);
      defaultSocketListeners.put(res, newDefaultSocketListener);
      return res;
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
    if (!availableConnections.containsKey(connection.getAddress())) {
      synchronized (availableConnections) {
        if (!availableConnections.containsKey(connection.getAddress()))
          availableConnections.put(connection.getAddress(), new ConcurrentLinkedDeque<>());
      }
    }

    defaultSocketListeners.get(connection).init(null);
    @SuppressWarnings("unchecked")
    Connection<? extends TServiceClient> c = (Connection<? extends TServiceClient>) connection;
    availableConnections.get(connection.getAddress()).add(c);
  }

  /**
   * Override connection factory for tests.
   */
  /* package */ void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void clusterInitialized() {
    // noop.
  }

  @Override
  public void nodeDied(RNodeAddress nodeAddr) {
    // ClusterManager informed us that a specific node died. Close all its connections and clean up all resources.
    // See the comment in ClusterManager#nodeDied for more info on when this procedure might get harmful in the future.
    Deque<Connection<? extends TServiceClient>> oldConnections = availableConnections.remove(nodeAddr);
    for (Connection<? extends TServiceClient> conn : oldConnections) {
      defaultSocketListeners.remove(conn);
      conn.getTransport().close();
    }

    // Note that it might happen that there are still reserved connections for the node - in which case we cannot clean
    // defaultSocketListeners right here. It is to be expected that those reserved connections though will throw
    // exceptions, too - and will end up being handled by DefaultSocketListener.
  }

  /**
   * A {@link SocketListener} that is installed on all connections returned by {@link ConnectionPool} and which is
   * maintained in {@link ConnectionPool#defaultSocketListeners}. The {@link DefaultSocketListener} can basically
   * forward any events that are thrown by the actual socket to any custom-installed delegate {@link SocketListener}s -
   * the latter ones change on each
   * {@link ConnectionPool#reserveConnection(Class, String, RNodeAddress, SocketListener)} and therefore we need to be
   * flexible here, because we do not want to close/open the sockets themselves all the time, but maintain open
   * connections.
   * 
   * The implementation takes care of marking nodes as "died" in the {@link ClusterManager} as soon as a connection
   * fails to a node.
   */
  private class DefaultSocketListener implements SocketListener {

    private RNodeAddress address;
    private SocketListener delegateListener;
    private Connection<? extends TServiceClient> parentConnection;

    public DefaultSocketListener(RNodeAddress address) {
      this.address = address;
    }

    public void init(SocketListener delegateListener) {
      this.delegateListener = delegateListener;
    }

    public void setParentConnection(Connection<? extends TServiceClient> parentConnection) {
      this.parentConnection = parentConnection;
    }

    @Override
    public void connectionDied() {
      logger.warn("Connection to {} died unexpectedly.", new NodeAddress(address));
      clusterManager.nodeDied(address);
      defaultSocketListeners.remove(parentConnection);

      if (delegateListener != null)
        delegateListener.connectionDied();
    }
  }

  /**
   * A thread that runs continuously and verifies the availability of remote nodes.
   * 
   * This is done only on connections that are not currently reserved.
   */
  private class KeepAliveThread extends Thread {
    private Object wait = new Object();

    public KeepAliveThread() {
      super("ConnectionPool-keepAlive");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      while (true) {
        try {
          synchronized (wait) {
            wait.wait(keepAlive);
          }
        } catch (InterruptedException e) {
          // interrupted, quielty say goodbye.
          return;
        }

        Set<RNodeAddress> addrs = new HashSet<>(availableConnections.keySet());
        for (RNodeAddress addr : addrs) {
          Deque<Connection<? extends TServiceClient>> conns = availableConnections.get(addr);
          if (conns == null)
            continue;

          Connection<? extends TServiceClient> conn = conns.poll();
          if (conn == null)
            // either there are no open connections to this address, or all connections are currently in use (=
            // connection is ok, no need to send keep alives).
            continue;

          // we reserved "conn".

          Connection<KeepAliveService.Client> keepAliveConn;
          if (conn.getService() instanceof ClusterManagementService.Iface)
            keepAliveConn = (Connection<KeepAliveService.Client>) conn;
          else {
            keepAliveConn = connectionFactory.createConnection(conn, KeepAliveService.Client.class,
                KeepAliveServiceConstants.SERVICE_NAME);

            defaultSocketListeners.put(keepAliveConn, defaultSocketListeners.get(conn));
            defaultSocketListeners.remove(conn);
          }

          try {
            keepAliveConn.getService().ping();

            // connection is alive! wohoo!
            // return connection
            releaseConnection(keepAliveConn);
          } catch (TException e) {
            // connection seems to be dead.
            // ClusterManager will be informed automatically (DefaultSocketListener does this).
            defaultSocketListeners.remove(keepAliveConn);
          }
        }
      }
    }

  }

}
