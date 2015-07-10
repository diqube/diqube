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
package org.diqube.plan.planner;

import java.util.Map;
import java.util.function.Supplier;

import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.plan.request.OrderRequest;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.util.Pair;

/**
 * Creates steps for {@link OrderRequest}s for cluster nodes.
 * <p>
 * These steps might actually not sort fully, because the ordering might depend on grouped columns, which are not
 * available on the cluster nodes. Therefore the ordering will only be executed in case there is a LIMIT clause -
 * because that would acutally help us, because the cluster nodes do not need to transfer as many rows any more, only
 * those that are within the limit. In that case, ordering will be done as well as possible, which means that the result
 * row IDs will be ordered using this Order terms up to the first order term that contains a grouped column.
 *
 * @author Bastian Gloeckle
 */
public class RemoteOrderHandler implements OrderRequestBuilder<RExecutionPlanStep> {

  private Map<String, PlannerColumnInfo> columnInfo;
  private Supplier<Integer> nextRemoteIdSupplier;
  private RemoteExecutionPlanFactory remoteExecutionPlanFactory;
  private ColumnManager<RExecutionPlanStep> columnManager;

  public RemoteOrderHandler(Map<String, PlannerColumnInfo> columnInfo, Supplier<Integer> nextRemoteIdSupplier,
      RemoteExecutionPlanFactory remoteExecutionPlanFactory, ColumnManager<RExecutionPlanStep> columnManager) {
    this.columnInfo = columnInfo;
    this.nextRemoteIdSupplier = nextRemoteIdSupplier;
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.columnManager = columnManager;
  }

  @Override
  public RExecutionPlanStep build(OrderRequest orderRequest) {
    // create a new OrderRequest that contains all columns up to the first aggregation column. We can safely order
    // that on the cluster nodes and apply LIMIT clauses - that would limit the number of items reported by each
    // cluster node to the query master. The query master itself can then sort based on the whole OrderRequest.
    if (orderRequest.getLimit() != null) {
      OrderRequest clusterNodeOrder =
          orderRequest.createSubOrderRequestUpTo(pair -> columnInfo.containsKey(pair.getLeft())
              && columnInfo.get(pair.getLeft()).getType().equals(FunctionRequest.Type.AGGREGATION));

      if (clusterNodeOrder.getLimitStart() != null) {
        // be sure that all results of the cluster nodes are included and sent to the query master
        clusterNodeOrder.setLimit(clusterNodeOrder.getLimit() + clusterNodeOrder.getLimitStart());
        clusterNodeOrder.setLimitStart(null);
      }

      if (clusterNodeOrder.getColumns().size() < orderRequest.getColumns().size()) {
        // the ordering contains a grouped column. This means that the remote cluster nodes cannot provide a full
        // ordering and cannot distinguish which lines should be included and which should be cut off for rows that have
        // equal values in the order-by-columns. We therefore will force the cluster nodes to include all those rows
        // which have equal values in the order-by-columns to any other row which has ordering index < limit. Define
        // this using a soft limit.
        clusterNodeOrder.setSoftLimit(clusterNodeOrder.getLimit());
        clusterNodeOrder.setLimit(null);
      }

      RExecutionPlanStep remoteOrderStep =
          remoteExecutionPlanFactory.createOrder(clusterNodeOrder, nextRemoteIdSupplier.get());

      // If we need to wait for specific columns to be built, wire that accordingly.
      for (Pair<String, Boolean> orderPair : clusterNodeOrder.getColumns())
        columnManager.wireOutputOfColumnIfAvailable(orderPair.getLeft(), remoteOrderStep);

      return remoteOrderStep;
    }

    return null;
  }
}
