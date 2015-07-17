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
package org.diqube.server.execution.util;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.cluster.thrift.ClusterNodeService;
import org.diqube.remote.cluster.thrift.RExecutionException;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.ROldNewIntermediateAggregationResult;
import org.diqube.server.queryremote.ClusterNodeServiceHandler;

/**
 * A {@link ClusterNodeService} that simply does nothing. Usable for unit tests, if the original
 * {@link ClusterNodeServiceHandler} bean in the context gets overridden.
 *
 * @author Bastian Gloeckle
 */
public class NoopClusterNodeService implements ClusterNodeService.Iface {

  @Override
  public void executeOnAllLocalShards(RExecutionPlan executionPlan, RUUID queryId, RNodeAddress resultAddress)
      throws TException {
  }

  @Override
  public void groupIntermediateAggregationResultAvailable(RUUID queryId, long groupId, String colName,
      ROldNewIntermediateAggregationResult result) throws TException {
  }

  @Override
  public void columnValueAvailable(RUUID queryId, String colName, Map<Long, RValue> valuesByRowId) throws TException {
  }

  @Override
  public void executionDone(RUUID queryId) throws TException {
  }

  @Override
  public void executionException(RUUID queryId, RExecutionException executionException) throws TException {
  }

  @Override
  public void hello(RNodeAddress newNode) throws TException {
  }

  @Override
  public Map<RNodeAddress, Map<Long, List<String>>> clusterLayout() throws TException {
    return null;
  }

  @Override
  public void newNodeData(RNodeAddress nodeAddr, long version, List<String> tables) throws TException {
  }

  @Override
  public void nodeDied(RNodeAddress nodeAddr) throws TException {
  }

}
