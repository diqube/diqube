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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.diqube.queries.QueryUuid;
import org.diqube.remote.base.thrift.RNodeAddress;
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

  @Config(ConfigKey.CONNECTION_SOFT_LIMIT)
  private int connectionSoftLimit;

  private KeepAliveThread keepAliveThread = new KeepAliveThread();

  private ConnectionFactory connectionFactory;

  /** This object will be {@link Object#notifyAll() notified} as soon as new connections became available */
  private Object connectionsAvailableWait = new Object();

  @Inject
  private ClusterManager clusterManager;

  /**
   * Connections by remote address which are currently not used. Each connection is fitted with a
   * {@link DefaultSocketListener} which can be fetched using {@link #defaultSocketListeners}.
   * 
   * If a connection dies while being reserved (= not being inside this map), just do not return the connection into
   * this map. Remember to remove resources of dead connections in {@link #defaultSocketListeners}.
   * 
   * The only wire-access to this map currently happens in {@link #releaseConnection(Connection)}. If this changes, it
   * needs further synchronization!
   */
  private Map<RNodeAddress, Deque<Connection<? extends TServiceClient>>> availableConnections =
      new ConcurrentHashMap<>();

  /**
   * The {@link DefaultSocketListener} that a specific connection is bound to.
   */
  private Map<Connection<? extends TServiceClient>, DefaultSocketListener> defaultSocketListeners =
      new ConcurrentHashMap<>();

  /**
   * The number of connections opened currently for a given execution UUID (see
   * {@link QueryUuid#getCurrentExecutionUuid()}.
   * 
   * The entries in this map will be removed as soon as the value reaches 0 - for this to work, there is additional
   * synchronization needed, which is done on the UUID object. See
   * {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)} and
   * {@link #decreaseOpenConnectionsByExecutionUuid(UUID)}.
   */
  private Map<UUID, AtomicInteger> openConnectionsByExecutionUuid = new ConcurrentHashMap<>();

  private AtomicInteger overallOpenConnections = new AtomicInteger(0);

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
   * Reserve a connection for the given remote address. This blocks until a new connection is available for the remote.
   * 
   * <p>
   * Note that for executions that already have a connection reserved, new connections will always be returned, no
   * matter if we reached the "max" of opened connections already. This is to not run into deadlocks while executing a
   * query. The execution that is currently being executed is identified by {@link QueryUuid#getCurrentExecutionUuid()}.
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
   * @throws InterruptedException
   *           in case we were interrupted while waiting for a new connection.
   */
  public <T extends TServiceClient> Connection<T> reserveConnection(Class<T> thriftClientClass,
      String thriftServiceName, RNodeAddress addr, SocketListener socketListener)
          throws ConnectionException, InterruptedException {

    // TODO #32: Close connections after they have not been used for some time.

    Connection<T> res = null;

    UUID executionUuid = QueryUuid.getCurrentExecutionUuid();
    if (executionUuid != null && openConnectionsByExecutionUuid.containsKey(executionUuid)) {
      // This execution already has at least 1 reserved connection. To make absolutely sure that we do not end up in a
      // deadlock, we give that execution another connection, not verifying connectionSoftLimit.

      res = reserveAvailableOrCreateConnection(thriftClientClass, thriftServiceName, addr, socketListener);
    }

    if (res == null) {
      // Check if we reached our soft limit.
      // In case we did, this while loop will block and re-check if there is an available connection as soon as a
      // connection is released.
      // Note that this is not 100% thread safe, as we might leave the while loop because overallOpenConnections is low
      // enough to not hit the limit, but we were not able to reserve an already opened connection. In that case, we
      // will open a new connection to the remote. This might happen in multiple threads simultaneously though and we
      // might therefore exceed the limit of connections (overallOpenConnections is only increased after actually
      // opening the new connection). This is though not as bad, as we have a "soft" limit only anyway. Therefore we
      // spare ourselves some synchronization logic.
      // We re-check openConnectionsByExecutionUuid regularly, as it might happen that two threads of the same execution
      // simultaneously call this method, but only one gets a connection returned and the other needs to wait. Be sure
      // to break that possible deadlock by letting the waiting one pass as soon as we know there is another connection
      // for the execution. If the first connection is already returned again, then there cannot be a
      // connection-deadlock for that execution and therefore we can let the second one wait.

      // try to reserve an already opened connection
      res = reserveAvailableConnection(thriftClientClass, thriftServiceName, addr, socketListener);
      while (res == null && overallOpenConnections.get() >= connectionSoftLimit
          && (executionUuid == null || !openConnectionsByExecutionUuid.containsKey(executionUuid))) {
        synchronized (connectionsAvailableWait) {
          connectionsAvailableWait.wait(1000);
        }

        // try to reserve an already opened connection (there might have been one returned just now).
        res = reserveAvailableConnection(thriftClientClass, thriftServiceName, addr, socketListener);
      }

      if (res == null)
        // we were not able to reserve a connection before, but we are allowed to open a new one now, so get a
        // connection no matter how.
        res = reserveAvailableOrCreateConnection(thriftClientClass, thriftServiceName, addr, socketListener);
    }

    if (executionUuid != null) {
      AtomicInteger openConnCount = openConnectionsByExecutionUuid.get(executionUuid);
      if (openConnCount == null) {
        synchronized (executionUuid) {
          if (!openConnectionsByExecutionUuid.containsKey(executionUuid))
            openConnectionsByExecutionUuid.put(executionUuid, new AtomicInteger(0));
          openConnCount = openConnectionsByExecutionUuid.get(executionUuid);
        }
      }

      // synchronize access with the decreaseOpenConnectionsByExecutionUuid method - make sure that there is a valid
      // object inside openConnectionsByExecutionUuid although one connection is reserved simultaneously to another one
      // being released.
      // We are safe to sync on the UUID object here, as everywhere the exact same UUID object is used (see QueryUuid).
      synchronized (executionUuid) {
        openConnCount.incrementAndGet();
        if (openConnectionsByExecutionUuid.get(executionUuid) != openConnCount)
          openConnectionsByExecutionUuid.put(executionUuid, openConnCount);
      }
    }

    res.setExecutionUuid(executionUuid);

    return res;
  }

  /**
   * Reserves a connection that was already opened to the given remote.
   * 
   * @param thriftClientClass
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param thriftServiceName
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param addr
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param socketListener
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @return the reserved connection or <code>null</code> in case there was no available connection.
   */
  @SuppressWarnings("unchecked")
  private <T extends TServiceClient> Connection<T> reserveAvailableConnection(Class<T> thriftClientClass,
      String thriftServiceName, RNodeAddress addr, SocketListener socketListener) {
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

    return null;
  }

  /**
   * Reserves an available connection to a remote or, in case there is none available, opens a new connection to that
   * remote. This method does not verify {@link #overallOpenConnections}, but just increases it in case a new connection
   * is created.
   * 
   * @param thriftClientClass
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param thriftServiceName
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param addr
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @param socketListener
   *          see {@link #reserveConnection(Class, String, RNodeAddress, SocketListener)}.
   * @return the reserved or freshly opened connection
   */
  private <T extends TServiceClient> Connection<T> reserveAvailableOrCreateConnection(Class<T> thriftClientClass,
      String thriftServiceName, RNodeAddress addr, SocketListener socketListener) {

    Connection<T> res = null;
    res = reserveAvailableConnection(thriftClientClass, thriftServiceName, addr, socketListener);

    if (res == null) {
      // create a new connection.
      try {
        // This is not 100% thread safe, we might open too many connections, because opening a connection and increasing
        // overallOpenConnections is not atomic. This though is not as bad, as we only have a "soft" limit.
        DefaultSocketListener newDefaultSocketListener = new DefaultSocketListener(addr);
        overallOpenConnections.incrementAndGet();

        res = connectionFactory.createConnection(thriftClientClass, thriftServiceName, addr, newDefaultSocketListener);
        newDefaultSocketListener.init(socketListener);
        newDefaultSocketListener.setParentConnection(res);
        defaultSocketListeners.put(res, newDefaultSocketListener);
      } catch (ConnectionException e) {
        logger.warn("Could not connect to {}.", new NodeAddress(addr));
        clusterManager.nodeDied(addr);
        throw new ConnectionException("Error connecting to " + addr, e);
      }
    }

    return res;
  }

  /**
   * Release a connection that was reserved using {@link #reserveConnection(Class, RNodeAddress)}.
   * 
   * This can safely be called from {@link KeepAliveThread}, too, if that thread does not have a execution Uuid set.
   * 
   * @param connection
   *          The connection returned by {@link #reserveConnection(Class, RNodeAddress)}.
   */
  public <T> void releaseConnection(Connection<T> connection) {
    if (!availableConnections.containsKey(connection.getAddress())) {
      synchronized (this) {
        if (!availableConnections.containsKey(connection.getAddress()))
          availableConnections.put(connection.getAddress(), new ConcurrentLinkedDeque<>());
      }
    }

    defaultSocketListeners.get(connection).init(null);
    @SuppressWarnings("unchecked")
    Connection<? extends TServiceClient> c = (Connection<? extends TServiceClient>) connection;
    // mark connection as available.
    availableConnections.get(connection.getAddress()).add(c);

    decreaseOpenConnectionsByExecutionUuid(connection.getExecutionUuid());
    c.setExecutionUuid(null);

    synchronized (connectionsAvailableWait) {
      connectionsAvailableWait.notifyAll();
    }
  }

  /**
   * Decreases the number of open connections that are reserved by a specific execution Uuid.
   * 
   * @param executionUuid
   *          may be <code>null</code>.
   */
  private void decreaseOpenConnectionsByExecutionUuid(UUID executionUuid) {
    // this is fine with KeepAliveThread.
    if (executionUuid != null) {
      int value = openConnectionsByExecutionUuid.get(executionUuid).decrementAndGet();
      if (value == 0)
        // sync see #reserveConnection
        synchronized (executionUuid) {
          if (openConnectionsByExecutionUuid.getOrDefault(executionUuid, new AtomicInteger(1)).get() == 0)
            openConnectionsByExecutionUuid.remove(executionUuid);
        }
    }
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
    for (Connection<? extends TServiceClient> conn : oldConnections)
      cleanupConnection(conn);

    // Note that it might happen that there are still reserved connections for the node - in which case we cannot clean
    // defaultSocketListeners right here. It is to be expected that those reserved connections though will throw
    // exceptions, too - and will end up being handled by DefaultSocketListener.
  }

  /**
   * Cleans up the connection which died/was closed.
   * 
   * @param conn
   *          Must not be available in {@link #availableConnections} any more when calling this method!
   */
  private void cleanupConnection(Connection<? extends TServiceClient> conn) {
    defaultSocketListeners.remove(conn);
    conn.getTransport().close();
    if (conn.getExecutionUuid() != null)
      decreaseOpenConnectionsByExecutionUuid(conn.getExecutionUuid());
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
      clusterManager.nodeDied(address); // will in turn call ConnectionPool#nodeDied.

      // although ConnectionPool#nodeDied was called, we need to cleanup this connection, because
      // ConnectionPool#nodeDied only works on availableConnections!
      cleanupConnection(parentConnection);

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
          if (conn.getService() instanceof KeepAliveService.Iface)
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
            // return connection, we're fine to call releaseConnection as we do not have set anything in QueryUuid.
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
