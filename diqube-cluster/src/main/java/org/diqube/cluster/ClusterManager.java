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
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayoutStateMachine.RemoveNode;
import org.diqube.cluster.ClusterLayoutStateMachine.SetTablesOfNode;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.ClusterNodeStatusDetailListener;
import org.diqube.connection.Connection;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionPool;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.consensus.ConsensusClusterNodeAddressProvider;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.consensus.DiqubeCopycatServer;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.listeners.ServingListener;
import org.diqube.listeners.TableLoadListener;
import org.diqube.listeners.providers.LoadedTablesProvider;
import org.diqube.listeners.providers.OurNodeAddressStringProvider;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Manages state of the diqube-server cluster, this nodes state in other cluster nodes and shares information about
 * that.
 * 
 * <p>
 * This class ensures that the information other nodes have about this node is correct, it manages our nodes address.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterManager implements ServingListener, TableLoadListener, OurNodeAddressStringProvider,
    ClusterNodeStatusDetailListener, OurNodeAddressProvider, ConsensusClusterNodeAddressProvider {
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

  /** will contain "this", too! */
  @InjectOptional
  private List<ClusterNodeStatusDetailListener> clusterNodeDiedListeners;

  private List<NodeAddress> consensusClusterNodes = new ArrayList<>();

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private DiqubeCopycatClient consensusClient;

  @Inject
  private DiqubeCopycatServer consensusServer;

  @Inject
  private LoadedTablesProvider loadedTablesProvider;

  @Inject
  private ExecutorManager executorManager;

  /**
   * Disable the methods of {@link ClusterNodeStatusDetailListener} on startup, until we have initially found some
   * cluster nodes.
   */
  private boolean clusterNodeStatusDetailListenerDisabled = true;

  private ExecutorService executorService;

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

    executorService = executorManager.newCachedThreadPool("clustermanager-%d", new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        logger.error("Error while executing asynchronous ClusterManager task", e);
        // swallow otherwise, as we'd like to continue as well as possible.
      }
    });
  }

  @PreDestroy
  public void cleanup() {
    if (executorService != null)
      executorService.shutdownNow();
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

    if (clusterNodesConfigString == null || "".equals(clusterNodesConfigString)) {
      logger.info("There are no cluster nodes configured, will therefore not connect anywhere.");
      if (clusterManagerListeners != null)
        clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }
    List<NodeAddress> initialClusterNodes = parseClusterNodes(this.clusterNodesConfigString);
    if (initialClusterNodes == null) {
      logger.warn("There are no cluster nodes configured, will therefore not connect anywhere.");
      if (clusterManagerListeners != null)
        clusterManagerListeners.forEach(l -> l.clusterInitialized());
      return;
    }

    logger.debug("Starting to communicate to cluster using the configured hosts ({})...", initialClusterNodes);

    try {
      // use the first node we can contact to fetch a list of all cluster nodes it knows. That list will later be used
      // to startup the consensus node.
      Set<RNodeAddress> allClusterNodes = new HashSet<>();
      for (NodeAddress nodeAddr : initialClusterNodes) {
        try (Connection<ClusterManagementService.Iface> conn = reserveConnection(nodeAddr)) {
          allClusterNodes.addAll(conn.getService().getAllKnownClusterNodes());
        } catch (ConnectionException | TException | IOException e) {
          logger.warn("Could not contact cluster node at {}.", nodeAddr, e);
        }
      }

      if (allClusterNodes.isEmpty()) {
        logger.warn("There are no cluster nodes alive, will therefore not connect anywhere.");
        if (clusterManagerListeners != null)
          clusterManagerListeners.forEach(l -> l.clusterInitialized());
        return;
      }

      allClusterNodes.forEach(remoteAddr -> consensusClusterNodes.add(new NodeAddress(remoteAddr)));
    } catch (InterruptedException e) {
      logger.error("Interrupted while starting to communicate with cluster", e);
      return;
    }

    logger.info("Gathered {} node addresses of the cluster (limit): {}", consensusClusterNodes.size(),
        Iterables.limit(consensusClusterNodes, 100));

    // enable activity when dead or alive nodes are identified.
    clusterNodeStatusDetailListenerDisabled = false;

    if (clusterManagerListeners != null)
      clusterManagerListeners.forEach(l -> l.clusterInitialized());
  }

  private Connection<ClusterManagementService.Iface> reserveConnection(NodeAddress addr)
      throws ConnectionException, InterruptedException {
    return connectionPool.reserveConnection(ClusterManagementService.Iface.class, addr.createRemote(),
        null /* node will be removed automatically from ClusterManager, therefore no separate listener needed */);
  }

  @Override
  public void localServerStoppedServing() {
    // noop.
  }

  @Override
  public void nodeDied(RNodeAddress diedAddr) {
    if (clusterNodeStatusDetailListenerDisabled)
      // Disable during startup, as we do not want to act on "dead" nodes of the config file.
      return;

    // This will typically be called when a connection to a node fails. We will not remove the node from the consensus
    // cluster (as that would allow split-brains), but we ensure that its information is removed from the clusterLayout
    // across the consensus cluster. That way, no connections will be opened to that cluster node for queries any more
    // etc. We have to ensure that we integrate the current information again as soon as the node gets back online (=the
    // node gets restarted which would be a normal join to the consensus cluster or if the e.g. network partition ends
    // and we can communicate with the node again without it re-joining the cluster).
    if (diedAddr.isSetDefaultAddr()) {
      NodeAddress addr = new NodeAddress(diedAddr);

      logger.trace("Cluster node died. Checking consensus cluster if we need to distribute that information...");

      // execute asynchronously, as this might take some time and we might even still be in startup (e.g. internal
      // consensus cluster server startup).
      executorService.execute(() -> {
        if (clusterLayout.isNodeKnown(addr)) {
          logger.info("Cluster node died: {}. Distributing information on changed cluster layout in consensus cluster.",
              addr);
          // This might actually be executed by multiple cluster nodes in parallel, but that does not hurt that much, as
          // node deaths should be rare.
          consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class).removeNode(RemoveNode.local(addr));
        } else
          logger.trace("Cluster node died. No need to distribute information since that node was unknown to the "
              + "consensus cluster anyway.");
      });
    }
  }

  @Override
  public void nodeAlive(RNodeAddress remoteNodeAddr) throws InterruptedException {
    if (clusterNodeStatusDetailListenerDisabled)
      // Disable during startup, as we are not yet interesting in "alive" nodes - we will receive cluster layout
      // information automatically if we join a cluster (= our consensus log will be filled) or if we're a single node
      // setup, there are no nodes anyway.
      return;

    // This will typically be called on the consensus master node when a new node joined or became alive again, as the
    // consensus master periodically sends keepAlives to all nodes. We ensure here that we get current information about
    // that new node.

    if (!consensusServer.isLeader())
      // Only let the consensus leader find new alive nodes. This is to reduce the number of times a new node is asked
      // to "publishLoadedTables" and also to limit the number of times "clusterLayout.isNodeKnown" is called: This is
      // pretty slow on non-leader nodes, but we will receive a lot of "nodeAlive" calls.
      return;

    if (remoteNodeAddr.isSetDefaultAddr()) {
      NodeAddress addr = new NodeAddress(remoteNodeAddr);
      if (!clusterLayout.isNodeKnown(addr)) {
        logger.info("Cluster node seems to be accessible now: {}. As we do not have information on the tables this "
            + "new node serves, we ask it to publicize that.", addr);

        try (Connection<ClusterManagementService.Iface> conn = reserveConnection(addr)) {
          conn.getService().publishLoadedTablesInConsensus();
        } catch (ConnectionException | TException | IOException e) {
          logger.warn("Could not contact cluster node at {}.", addr, e);
        }
      }
    }
  }

  @Override
  public synchronized void tableLoaded(String newTableName) {
    logger.info("Informing consensus cluster of our updated table list.");
    consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)
        .setTablesOfNode(SetTablesOfNode.local(ourHostAddr, loadedTablesProvider.getNamesOfLoadedTables()));
    logger.trace("Informed consensus cluster of our updated table list.");
  }

  @Override
  public void tableUnloaded(String tableName) {
    logger.info("Informing consensus cluster of our updated table list.");
    consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)
        .setTablesOfNode(SetTablesOfNode.local(ourHostAddr, loadedTablesProvider.getNamesOfLoadedTables()));
    logger.trace("Informed consensus cluster of our updated table list.");
  }

  @Override
  public NodeAddress getOurNodeAddress() {
    return ourHostAddr;
  }

  @Override
  public String getOurNodeAddressAsString() {
    return ourHostAddr.toString();
  }

  @Override
  public List<NodeAddress> getClusterNodeAddressesForConsensus() {
    return consensusClusterNodes;
  }

}