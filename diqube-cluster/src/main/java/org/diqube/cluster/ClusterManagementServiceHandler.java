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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayoutStateMachine.SetTablesOfNode;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.consensus.DiqubeCopycatClient;
import org.diqube.context.AutoInstatiate;
import org.diqube.listeners.providers.LoadedTablesProvider;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@link ClusterManagementService}, which allows us bootstrapping into a cluster.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterManagementServiceHandler implements ClusterManagementService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterManagementServiceHandler.class);

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private DiqubeCopycatClient consensusClient;

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  @Inject
  private LoadedTablesProvider loadedTablesProvider;

  @Override
  public void publishLoadedTablesInConsensus() throws TException {
    logger.info("Publishing information on currently loaded tables in consensus, because we were requested to do so.");
    consensusClient.getStateMachineClient(ClusterLayoutStateMachine.class).setTablesOfNode(SetTablesOfNode
        .local(ourNodeAddressProvider.getOurNodeAddress(), loadedTablesProvider.getNamesOfLoadedTables()));
  }

  @Override
  public List<RNodeAddress> getAllKnownClusterNodes() throws TException {
    List<RNodeAddress> res = new ArrayList<>();
    // do not query the consensus cluster but only return the quick locally known nodes. This is needed, because this
    // method is called to bootstrap the consensus server of a new node -> we must not take too long and actually our
    // local list should be good enough for that.
    res.addAll(clusterLayout.getNodesInsecure().stream().map(a -> a.createRemote()).collect(Collectors.toList()));
    res.add(ourNodeAddressProvider.getOurNodeAddress().createRemote());
    return res;
  }

}
