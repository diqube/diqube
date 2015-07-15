package org.diqube.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.ConnectionPool.Connection;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.listeners.ServingListener;
import org.diqube.listeners.TableLoadListener;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.util.RNodeAddressUtil;
import org.diqube.remote.cluster.ClusterNodeServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterNodeService;
import org.diqube.util.Pair;
import org.diqube.util.RandomManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages knowledge of other nodes in the diqube cluster.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterManager implements ServingListener, TableLoadListener {
  private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

  private static final String OUR_HOST_AUTOMATIC = "*";

  @Config(ConfigKey.OUR_HOST)
  private String ourHost;

  @Config(ConfigKey.PORT)
  private int ourPort;

  private Pair<String, Short> ourHostPair;

  @Config(ConfigKey.CLUSTER_NODES)
  private String clusterNodesConfigString;

  @Inject
  private ConnectionPool connectionPool;

  @Inject
  private List<ClusterManagerListener> clusterManagerListeners;

  @Inject
  private RandomManager randomManager;

  /**
   * Map from remote address to Deque of tablenames of tables it serves shards of.
   */
  private Map<Pair<String, Short>, Deque<String>> clusterLayout = new ConcurrentHashMap<>();

  @PostConstruct
  public void initialize() {
    if (ourHost.equals(OUR_HOST_AUTOMATIC)) {
      try {
        InetAddress foundAddr = InetAddress.getLocalHost();
        ourHost = foundAddr.getHostAddress();
        logger.info("Using {} as our host address. We expect that other cluster nodes will be able to reach this "
            + "node under that address. If not, define a different host in the configuration!", ourHost);
      } catch (UnknownHostException e) {
        logger.error("Configuration said to identify our host automatically, "
            + "but was not able to inspect network interfaces.", e);
        throw new RuntimeException("Configuration said to identify our host automatically, "
            + "but was not able to inspect network interfaces.", e);
      }
    } else
      logger.info("Using {} as our host address. We expect that other cluster nodes will be able to reach this node "
          + "under that address!", ourHost);
  }

  private List<Pair<String, Short>> parseClusterNodes(String clusterNodes) {
    List<Pair<String, Short>> res = new ArrayList<>();

    for (String clusterNodeString : clusterNodes.split(",")) {
      int lastColon = clusterNodeString.lastIndexOf(":");
      if (lastColon == -1) {
        logger.warn("No port specified in '{}'. Ignoring.", clusterNodeString);
        continue;
      }
      if (lastColon == 0) {
        logger.warn("No host specified in '{}'. Ignoring.", clusterNodeString);
        continue;
      }
      short port;
      try {
        port = Short.valueOf(clusterNodeString.substring(lastColon + 1));
      } catch (NumberFormatException e) {
        logger.warn("Could not parse port in '{}'. Ignoring.", clusterNodeString);
        continue;
      }
      String host = clusterNodeString.substring(0, lastColon);

      res.add(new Pair<>(host, port));
    }

    if (res.isEmpty())
      return null;

    return res;
  }

  @Override
  public void localServerStartedServing() {
    // start sending hello messages etc as soon as our server is up and running so we can receive answers.

    ourHostPair = new Pair<>(ourHost, (short) ourPort);
    clusterLayout.put(ourHostPair, new ConcurrentLinkedDeque<>());

    if (clusterNodesConfigString == null || "".equals(clusterNodesConfigString)) {
      logger.info("There are no cluster nodes configured, will therefore not connect anywhere.");
      clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }
    List<Pair<String, Short>> clusterNodes = parseClusterNodes(this.clusterNodesConfigString);
    if (clusterNodes == null) {
      logger.warn("There are no cluster nodes configured, will therefore not connect anywhere.");
      clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }
    RNodeAddress ourAddress = RNodeAddressUtil.buildDefault(ourHost, (short) ourPort);

    // interact with other nodes in a separate "bootstrap" thread, as this might take some time - to not block
    // Thrifts thread here too long...
    new Thread(new Runnable() {
      @Override
      public void run() {
        List<Pair<String, Short>> workingRemoteAddr = new ArrayList<>();

        for (Pair<String, Short> nodeAddrPair : clusterNodes) {
          Connection<ClusterNodeService.Client> conn = null;
          try {
            RNodeAddress addr = RNodeAddressUtil.buildDefault(nodeAddrPair);
            conn = connectionPool.reserveConnection(ClusterNodeService.Client.class,
                ClusterNodeServiceConstants.SERVICE_NAME, addr);

            conn.getService().hello(ourAddress);
            workingRemoteAddr.add(nodeAddrPair);
          } catch (TException e) {
            logger.warn("Could not say hello to cluster node at {}:{}, will ignore that node for now.",
                nodeAddrPair.getLeft(), nodeAddrPair.getRight(), e);
          } finally {
            if (conn != null)
              connectionPool.releaseConnection(conn);
          }
        }

        if (workingRemoteAddr.isEmpty())
          logger.warn("There are no live cluster nodes, will therefore not connect anywhere.");
        else {
          Pair<String, Short> clusterLayoutAddr =
              workingRemoteAddr.get(randomManager.nextInt(workingRemoteAddr.size()));
          logger.info("Fetching cluster layout data from {}:{}.", clusterLayoutAddr.getLeft(),
              clusterLayoutAddr.getRight());

          RNodeAddress addr = RNodeAddressUtil.buildDefault(clusterLayoutAddr);
          Connection<ClusterNodeService.Client> conn = null;
          try {
            conn = connectionPool.reserveConnection(ClusterNodeService.Client.class,
                ClusterNodeServiceConstants.SERVICE_NAME, addr);
            Map<RNodeAddress, List<String>> newClusterLayout = conn.getService().clusterLayout();

            for (Entry<RNodeAddress, List<String>> layoutEntry : newClusterLayout.entrySet())
              loadNodeInfo(layoutEntry.getKey(), layoutEntry.getValue());

            logger.info("Loaded cluster data of {} nodes: {}", clusterLayout.size(), clusterLayout.keySet());
          } catch (TException e) {
            // TODO remote node could go down right now.
            logger.error("Could not retrieve cluster layout from {}:{}.", clusterLayoutAddr.getLeft(),
                clusterLayoutAddr.getRight());
          } finally {
            if (conn != null)
              connectionPool.releaseConnection(conn);
          }

          // say hello to all nodes
          for (Pair<String, Short> remotePair : clusterLayout.keySet()) {
            if (remotePair.equals(ourHostPair))
              continue;

            conn = null;
            addr = RNodeAddressUtil.buildDefault(remotePair);
            try {
              conn = connectionPool.reserveConnection(ClusterNodeService.Client.class,
                  ClusterNodeServiceConstants.SERVICE_NAME, addr);

              // TODO send hash of table-name-list we have of the node - if anything changed in the meantime, we
              // should fetch the new info.
              conn.getService().hello(ourAddress);
            } catch (TException e) {
              logger.error("Could not say hello to node {}:{}.", clusterLayoutAddr.getLeft(),
                  clusterLayoutAddr.getRight());
              // TODO mark node as dead.
            } finally {
              if (conn != null)
                connectionPool.releaseConnection(conn);
            }
          }
        }

        clusterManagerListeners.forEach(l -> l.clusterInitialized());
      }
    }, "cluster-bootstrap").start();
  }

  @Override
  public void localServerStoppedServing() {
    // TODO try to tell everybody that we are dying
  }

  /**
   * Set the given list of table names as our clusterLayout information for the given node.
   */
  private void loadNodeInfo(RNodeAddress nodeAddr, List<String> tables) {
    Pair<String, Short> addr = RNodeAddressUtil.readDefault(nodeAddr);

    if (!clusterLayout.containsKey(addr) || !clusterLayout.get(addr).isEmpty())
      clusterLayout.put(addr, new ConcurrentLinkedDeque<>());
    clusterLayout.get(addr).addAll(tables);
  }

  /**
   * Called when a new cluster node came online.
   */
  public void newNode(RNodeAddress newNodeAddress) {
    Pair<String, Short> addr = RNodeAddressUtil.readDefault(newNodeAddress);

    if (!clusterLayout.containsKey(addr)) {
      logger.info("Learned about a new cluster node being online: {}:{}.", addr.getLeft(), addr.getRight());
      clusterLayout.put(addr, new ConcurrentLinkedDeque<>());
    }
  }

  /**
   * We are informed that a specific node died.
   */
  public void nodeDied(RNodeAddress diedAddr) {
    Pair<String, Short> addr = RNodeAddressUtil.readDefault(diedAddr);

    clusterLayout.remove(addr);

    // TODO inform other nodes about dead node.
  }

  /**
   * @return Currently known cluster layout, mapping from remote address to list of tablesnames we know that remote
   *         serves shards of.
   */
  public Map<RNodeAddress, List<String>> getClusterLayout() {
    Map<RNodeAddress, List<String>> res = new HashMap<>();

    for (Entry<Pair<String, Short>, Deque<String>> entry : clusterLayout.entrySet()) {
      RNodeAddress addr = RNodeAddressUtil.buildDefault(entry.getKey());
      res.put(addr, new ArrayList<>(entry.getValue().size()));
      res.get(addr).addAll(entry.getValue());
    }

    return res;
  }

  @Override
  public synchronized void tableShardLoaded(String tableName) {
    if (!clusterLayout.get(ourHostPair).contains(tableName)) {
      clusterLayout.get(ourHostPair).add(tableName);
      // TODO inform other nodes.
    }
  }

  @Override
  public void tableShardUnloaded(String tableName, boolean nodeStillContainsOtherShard) {
    if (!nodeStillContainsOtherShard) {
      clusterLayout.get(ourHostPair).remove(tableName);
      // TODO inform other nodes.
    }
  }
}
