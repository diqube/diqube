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
package org.diqube.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.connection.Connection;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.ConnectionPool.ConnectionException;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.listeners.ServingListener;
import org.diqube.listeners.TableLoadListener;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.util.RNodeAddressUtil;
import org.diqube.remote.cluster.ClusterManagementServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
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

  private NodeAddress ourHostAddr;

  @Config(ConfigKey.CLUSTER_NODES)
  private String clusterNodesConfigString;

  @Inject
  private ConnectionPool connectionPool;

  @InjectOptional
  private List<ClusterManagerListener> clusterManagerListeners;

  @Inject
  private RandomManager randomManager;

  private ClusterLayout clusterLayout = new ClusterLayout();

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

    ourHostAddr = new NodeAddress(ourHost, (short) ourPort);
    clusterLayout.addNode(ourHostAddr);
  }

  private List<NodeAddress> parseClusterNodes(String clusterNodes) {
    List<NodeAddress> res = new ArrayList<>();

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

      res.add(new NodeAddress(host, port));
    }

    if (res.isEmpty())
      return null;

    return res;
  }

  @Override
  public void localServerStartedServing() {
    // start sending hello messages etc as soon as our server is up and running so we can receive answers.

    if (clusterNodesConfigString == null || "".equals(clusterNodesConfigString)) {
      logger.info("There are no cluster nodes configured, will therefore not connect anywhere.");
      if (clusterManagerListeners != null)
        clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }
    List<NodeAddress> clusterNodes = parseClusterNodes(this.clusterNodesConfigString);
    if (clusterNodes == null) {
      logger.warn("There are no cluster nodes configured, will therefore not connect anywhere.");
      if (clusterManagerListeners != null)
        clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }
    RNodeAddress ourAddress = RNodeAddressUtil.buildDefault(ourHost, (short) ourPort);

    // interact with other nodes in a separate "bootstrap" thread, as this might take some time - to not block
    // Thrifts thread here too long...
    new Thread(new Runnable() {
      @Override
      public void run() {
        List<NodeAddress> workingRemoteAddr = new ArrayList<>();

        for (NodeAddress nodeAddr : clusterNodes) {
          try (Connection<ClusterManagementService.Client> conn = reserveConnection(nodeAddr)) {
            conn.getService().hello(ourAddress);
            workingRemoteAddr.add(nodeAddr);
          } catch (ConnectionException | TException | IOException e) {
            logger.warn("Could not say hello to cluster node at {}, will ignore that node for now.", nodeAddr, e);
          }
        }

        logger.info("Greeted cluster nodes {}", workingRemoteAddr);

        if (workingRemoteAddr.isEmpty())
          logger.warn("There are no cluster nodes alive, will therefore not connect anywhere.");
        else {
          boolean fetchedClusterLayout = false;
          Set<NodeAddress> visitedRemoteNodesForLayout = new HashSet<>();
          while (!fetchedClusterLayout && visitedRemoteNodesForLayout.size() < workingRemoteAddr.size()) {
            NodeAddress clusterLayoutAddr = null;
            while (clusterLayoutAddr == null || visitedRemoteNodesForLayout.contains(clusterLayoutAddr))
              clusterLayoutAddr = workingRemoteAddr.get(randomManager.nextInt(workingRemoteAddr.size()));
            visitedRemoteNodesForLayout.add(clusterLayoutAddr);
            logger.info("Fetching cluster layout data from {}", clusterLayoutAddr);

            try (Connection<ClusterManagementService.Client> conn = reserveConnection(clusterLayoutAddr)) {
              Map<RNodeAddress, Map<Long, List<String>>> newClusterLayout = conn.getService().clusterLayout();

              for (Entry<RNodeAddress, Map<Long, List<String>>> layoutEntry : newClusterLayout.entrySet()) {
                // layoutEntry.getValue() is a map containing only a single entry (see .thrift file!).
                long version = layoutEntry.getValue().keySet().iterator().next();
                List<String> tables = layoutEntry.getValue().get(version);
                loadNodeInfo(layoutEntry.getKey(), version, tables);
              }

              logger.info("Loaded cluster data from {} of {} nodes: {}", clusterLayoutAddr,
                  clusterLayout.getNumberOfNodes(), clusterLayout.getNodes());
              fetchedClusterLayout = true;
            } catch (ConnectionException | TException | IOException e) {
              logger.error("Could not retrieve cluster layout from {}.", clusterLayoutAddr);
            }
          }

          if (!fetchedClusterLayout) {
            // ConnectionPool guarantees that it has removed those nodes that we were not able to access from this
            // ClusterManager already, therefore ClusterLayout is empty now (contains only our node).
            logger.warn("I was able to say hello to at least one cluster node, but was unable to retrieve the cluster "
                + "layout from all of them. I am now not connected to any node.");
          } else {
            logger.info("Starting to greet all cluster nodes I know.");
            // say hello to all nodes
            for (NodeAddress remoteAddr : clusterLayout.getNodes()) {
              if (remoteAddr.equals(ourHostAddr))
                continue;

              try (Connection<ClusterManagementService.Client> conn = reserveConnection(remoteAddr)) {
                long version = conn.getService().hello(ourAddress);

                if (version != clusterLayout.getVersionedTableList(remoteAddr).getLeft()) {
                  // node already has new list of tables, fetch new list, as we might miss it otherwise.
                  Map<Long, List<String>> newTables = conn.getService().fetchCurrentTablesServed();
                  long newVersion = newTables.keySet().iterator().next();
                  loadNodeInfo(remoteAddr.createRemote(), newVersion, newTables.get(version));
                }
              } catch (ConnectionException | TException | IOException e) {
                // swallow, in case an exception happens, this will be handled automatically by the default listeners in
                // ConnectionPool.
              }
            }
          }
        }

        if (clusterManagerListeners != null)
          clusterManagerListeners.forEach(l -> l.clusterInitialized());
      }
    }, "cluster-bootstrap").start();
  }

  private Connection<ClusterManagementService.Client> reserveConnection(NodeAddress addr) throws ConnectionException {
    return connectionPool.reserveConnection(ClusterManagementService.Client.class,
        ClusterManagementServiceConstants.SERVICE_NAME, addr.createRemote(),
        null /* node will be removed automatically from ClusterManager, therefore no separate listener needed */);
  }

  @Override
  public void localServerStoppedServing() {
    // Inform as much nodes as possible that we died.
    RNodeAddress ourRemoteAddr = ourHostAddr.createRemote();
    for (NodeAddress addr : clusterLayout.getNodes()) {
      if (addr.equals(ourHostAddr))
        continue;

      try (Connection<ClusterManagementService.Client> conn = reserveConnection(addr)) {
        conn.getService().nodeDied(ourRemoteAddr);
      } catch (ConnectionException | IOException | TException e) {
        // swallow, in case an exception happens, this will be handled automatically by the default listeners in
        // ConnectionPool.
      }
    }
  }

  /**
   * Set the given list of table names as our clusterLayout information for the given node.
   */
  public void loadNodeInfo(RNodeAddress nodeAddr, long version, List<String> tables) {
    NodeAddress addr = new NodeAddress(nodeAddr);
    if (clusterLayout.setTables(addr, version, tables) && !addr.equals(ourHostAddr))
      logger.info("Updated list of tables node {} serves (version {}): {}", addr, version, tables);
  }

  /**
   * Called when a new cluster node came online.
   */
  public void newNode(RNodeAddress newNodeAddress) {
    // we might know about that node already, though.
    NodeAddress addr = new NodeAddress(newNodeAddress);
    if (!clusterLayout.addNode(addr) && !addr.equals(ourHostAddr))
      logger.info("New cluster node {}", addr);
  }

  /**
   * We are informed that a specific node died.
   */
  public void nodeDied(RNodeAddress diedAddr) {
    NodeAddress addr = new NodeAddress(diedAddr);
    if (clusterLayout.removeNode(addr) && !addr.equals(ourHostAddr))
      logger.info("Cluster node died: {}. Will not send any requests to that node any more.", addr);
  }

  public ClusterLayout getClusterLayout() {
    return clusterLayout;
  }

  @Override
  public synchronized void tableShardLoaded(String tableName) {
    boolean isNewTable = clusterLayout.addTable(ourHostAddr, tableName) != null;

    if (isNewTable) {
      RNodeAddress ourRemoteAddr = ourHostAddr.createRemote();
      Pair<Long, List<String>> versionedTableList = clusterLayout.getVersionedTableList(ourHostAddr);

      if (clusterLayout.getNodes().size() > 1)
        logger.info("Informing other cluster nodes about updated list of tables served by this node (version {})",
            versionedTableList.getLeft());

      for (NodeAddress addr : clusterLayout.getNodes()) {
        if (addr.equals(ourHostAddr))
          continue;

        try (Connection<ClusterManagementService.Client> conn = reserveConnection(addr)) {
          conn.getService().newNodeData(ourRemoteAddr, versionedTableList.getLeft(), versionedTableList.getRight());
        } catch (ConnectionException | IOException | TException e) {
          // swallow, in case an exception happens, this will be handled automatically by the default listeners in
          // ConnectionPool.
        }
      }
    }
  }

  @Override
  public void tableShardUnloaded(String tableName, boolean nodeStillContainsOtherShard) {
    if (!nodeStillContainsOtherShard) {
      boolean tableWasRemoved = clusterLayout.removeTable(ourHostAddr, tableName) != null;
      if (tableWasRemoved) {
        RNodeAddress ourRemoteAddr = ourHostAddr.createRemote();
        Pair<Long, List<String>> versionedTableList = clusterLayout.getVersionedTableList(ourHostAddr);

        if (clusterLayout.getNodes().size() > 1)
          logger.info("Informing other cluster nodes about updated list of tables served by this node.");

        for (NodeAddress addr : clusterLayout.getNodes()) {
          if (addr.equals(ourHostAddr))
            continue;

          try (Connection<ClusterManagementService.Client> conn = reserveConnection(addr)) {
            conn.getService().newNodeData(ourRemoteAddr, versionedTableList.getLeft(), versionedTableList.getRight());
          } catch (ConnectionException | IOException | TException e) {
            // swallow, in case an exception happens, this will be handled automatically by the default listeners in
            // ConnectionPool.
          }
        }
      }
    }
  }

  public NodeAddress getOurHostAddr() {
    return ourHostAddr;
  }
}
