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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.util.Pair;

/**
 * Implements {@link ClusterManagementService}, which manages the various nodes of a diqube cluster.
 * 
 * When executing queries, the of this service methods will be called on the "query remote" nodes.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterManagementServiceHandler implements ClusterManagementService.Iface {
  @Inject
  private ClusterManager clusterManager;

  /**
   * A new cluster node says "hello".
   * 
   * @return the current version number of the table-list this node serves parts of.
   */
  @Override
  public long hello(RNodeAddress newNode) throws TException {
    clusterManager.newNode(newNode);
    return clusterManager.getClusterLayout().getVersionedTableList(clusterManager.getOurHostAddr()).getLeft();
  }

  /**
   * Someone asks us what cluster nodes we know and what tables they serve shards of.
   */
  @Override
  public Map<RNodeAddress, Map<Long, List<String>>> clusterLayout() throws TException {
    return clusterManager.getClusterLayout().createRemoteLayout();
  }

  /**
   * A cluster node has an updated list of tables available for which it serves data.
   */
  @Override
  public void newNodeData(RNodeAddress nodeAddr, long version, List<String> tables) throws TException {
    clusterManager.loadNodeInfo(nodeAddr, version, tables);
  }

  /**
   * A cluster node died.
   */
  @Override
  public void nodeDied(RNodeAddress nodeAddr) throws TException {
    clusterManager.nodeDied(nodeAddr);
  }

  /**
   * @return single entry map containing the current version and current list of table names this node serves parts of.
   */
  @Override
  public Map<Long, List<String>> fetchCurrentTablesServed() throws TException {
    Pair<Long, List<String>> p =
        clusterManager.getClusterLayout().getVersionedTableList(clusterManager.getOurHostAddr());
    Map<Long, List<String>> res = new HashMap<>();
    res.put(p.getLeft(), p.getRight());
    return res;
  }

}
