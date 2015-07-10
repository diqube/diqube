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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;

/**
 * Manages steps resolving column values on cluster nodes.
 *
 * @author Bastian Gloeckle
 */
public class RemoteResolveManager implements ResolveManager<RExecutionPlanStep> {
  private List<RExecutionPlanStep> steps = new ArrayList<RExecutionPlanStep>();
  private Supplier<Integer> nextRemoteIdSupplier;
  private ColumnManager<RExecutionPlanStep> columnManager;
  private RemoteExecutionPlanFactory remoteExecutionPlanFactory;
  private Set<String> resolvedCols = new HashSet<>();
  private RemoteWireManager remoteWireManager;

  public RemoteResolveManager(Supplier<Integer> nextRemoteIdSupplier, ColumnManager<RExecutionPlanStep> columnManager,
      RemoteExecutionPlanFactory remoteExecutionPlanFactory, RemoteWireManager remoteWireManager) {
    this.nextRemoteIdSupplier = nextRemoteIdSupplier;
    this.columnManager = columnManager;
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.remoteWireManager = remoteWireManager;
  }

  @Override
  public void resolveValuesOfColumn(String colName) {
    if (resolvedCols.contains(colName))
      return;
    resolvedCols.add(colName);

    RExecutionPlanStep resolveStep =
        remoteExecutionPlanFactory.createResolveColumnDictId(colName, nextRemoteIdSupplier.get());
    columnManager.wireOutputOfColumnIfAvailable(colName, resolveStep);
    steps.add(resolveStep);
  }

  @Override
  public List<RExecutionPlanStep> build(RExecutionPlanStep rowIdSourceStep) {
    // columnValueSourceStep ignored.
    if (steps.size() > 0) {
      RExecutionPlanStep resolveValueStep = remoteExecutionPlanFactory.createResolveValues(nextRemoteIdSupplier.get());
      for (RExecutionPlanStep dictIdStep : steps) {
        // input: provide RowIDs to dict ID step
        remoteWireManager.wire(RowIdConsumer.class, rowIdSourceStep, dictIdStep);

        // output: provide Dict IDs to resolveValue step
        remoteWireManager.wire(ColumnDictIdConsumer.class, dictIdStep, resolveValueStep);
      }
      steps.add(resolveValueStep);
    }
    return steps;
  }
}