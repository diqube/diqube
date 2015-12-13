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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.diqube.cluster.ClusterLayoutStateMachine.FindNodesServingTable;
import org.diqube.cluster.ClusterLayoutStateMachine.GetAllNodes;
import org.diqube.cluster.ClusterLayoutStateMachine.GetAllTablesServed;
import org.diqube.cluster.ClusterLayoutStateMachine.IsNodeKnown;
import org.diqube.connection.NodeAddress;
import org.diqube.consensus.ConsensusStateMachineClientInterruptedException;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains addresses of all cluster nodes known and the tables the respective node is serving data of.
 * 
 * This includes information about our node.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterLayout {
  private static final Logger logger = LoggerFactory.getLogger(ClusterLayout.class);

  @Inject
  private ConsensusClient consensusClient;

  @Inject
  private ClusterLayoutStateMachineImplementation clusterLayoutStateMachineImplementation;

  /**
   * @return Addresses of all cluster nodes currently applied to this nodes' {@link ClusterLayoutStateMachine}. It can
   *         therefore contain nodes which are not alive anymore and may not contain all live nodes.
   */
  public Set<NodeAddress> getNodesInsecure() {
    return clusterLayoutStateMachineImplementation.getLocalKnownNodesInsecure();
  }

  /**
   * @return Addresses of all cluster nodes that are known (including our node).
   */
  public Set<NodeAddress> getNodes() throws InterruptedException {
    try (ClosableProvider<ClusterLayoutStateMachine> p =
        consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)) {

      return new HashSet<>(p.getClient().getAllNodes(GetAllNodes.local()));

    } catch (ConsensusStateMachineClientInterruptedException e) {
      logger.error("Interrupted.", e);
      throw e.getInterruptedException();
    }
  }

  /**
   * @return true if the layout knows that the given node is alive. Note that when executed on nodes that are not the
   *         consensus master, this might be slow, despite it is expected to be quick!
   */
  public boolean isNodeKnown(NodeAddress addr) throws InterruptedException {
    try (ClosableProvider<ClusterLayoutStateMachine> p =
        consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)) {

      return p.getClient().isNodeKnown(IsNodeKnown.local(addr));

    } catch (ConsensusStateMachineClientInterruptedException e) {
      logger.error("Interrupted.", e);
      throw e.getInterruptedException();
    }
  }

  /**
   * Finds the addresses of nodes of which is known that they serve parts of a specific table.
   */
  public Collection<RNodeAddress> findNodesServingTable(String table) throws InterruptedException {
    try (ClosableProvider<ClusterLayoutStateMachine> p =
        consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)) {
      Set<NodeAddress> res = p.getClient().findNodesServingTable(FindNodesServingTable.local(table));

      return res.stream().map(addr -> addr.createRemote()).collect(Collectors.toSet());
    } catch (ConsensusStateMachineClientInterruptedException e) {
      logger.error("Interrupted.", e);
      throw e.getInterruptedException();
    }
  }

  /**
   * @return A set with all tablenames that are served from at least one cluster node.
   */
  public Set<String> getAllTablesServed() throws InterruptedException {
    try (ClosableProvider<ClusterLayoutStateMachine> p =
        consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class)) {

      return p.getClient().getAllTablesServed(GetAllTablesServed.local());

    } catch (ConsensusStateMachineClientInterruptedException e) {
      logger.error("Interrupted.", e);
      throw e.getInterruptedException();
    }
  }

}
