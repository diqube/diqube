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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;

import com.google.common.collect.Iterables;

/**
 * Manages the creation of columns on the cluster nodes.
 *
 * @author Bastian Gloeckle
 */
public class RemoteColumnManager implements ColumnManager<RExecutionPlanStep> {
  private Map<String, List<RExecutionPlanStep>> functionRemoteSteps = new HashMap<>();
  private Supplier<Integer> nextRemoteStepIdSupplier;
  private RemoteExecutionPlanFactory remoteExecutionPlanFactory;
  private Map<String, PlannerColumnInfo> columnInfo;
  private RemoteWireManager remoteWireManager;

  public RemoteColumnManager(Supplier<Integer> nextRemoteStepIdSupplier,
      RemoteExecutionPlanFactory remoteExecutionPlanFactory, Map<String, PlannerColumnInfo> columnInfo,
      RemoteWireManager remoteWireManager) {
    this.nextRemoteStepIdSupplier = nextRemoteStepIdSupplier;
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.columnInfo = columnInfo;
    this.remoteWireManager = remoteWireManager;
  }

  @Override
  public void produceColumn(FunctionRequest fnReq) {
    if (fnReq.getType().equals(FunctionRequest.Type.AGGREGATION_ROW)) {
      RExecutionPlanStep intermediaryStep =
          remoteExecutionPlanFactory.createGroupIntermediaryAggregateStep(fnReq, nextRemoteStepIdSupplier.get());

      functionRemoteSteps.put(fnReq.getOutputColumn(),
          new ArrayList<>(Arrays.asList(new RExecutionPlanStep[] { intermediaryStep })));
    } else if (fnReq.getType().equals(FunctionRequest.Type.AGGREGATION_COL)) {
      RExecutionPlanStep colAggStep =
          remoteExecutionPlanFactory.createColumnAggregateStep(fnReq, nextRemoteStepIdSupplier.get());

      functionRemoteSteps.put(fnReq.getOutputColumn(), new ArrayList<>(Arrays.asList(colAggStep)));
    } else if (fnReq.getType().equals(FunctionRequest.Type.PROJECTION)) {
      RExecutionPlanStep projectStep =
          remoteExecutionPlanFactory.createProjectStep(fnReq, nextRemoteStepIdSupplier.get());

      functionRemoteSteps.put(fnReq.getOutputColumn(),
          new ArrayList<>(Arrays.asList(new RExecutionPlanStep[] { projectStep })));
    } else if (fnReq.getType().equals(FunctionRequest.Type.REPEATED_PROJECTION)) {
      RExecutionPlanStep repeatedProjectStep =
          remoteExecutionPlanFactory.createRepeatedProjectStep(fnReq, nextRemoteStepIdSupplier.get());

      functionRemoteSteps.put(fnReq.getOutputColumn(),
          new ArrayList<>(Arrays.asList(new RExecutionPlanStep[] { repeatedProjectStep })));
    }
  }

  @Override
  public void ensureColumnAvailable(String colName) {
    // noop in remote.
  }

  @Override
  public void wireOutputOfColumnIfAvailable(String colName, RExecutionPlanStep targetStep) {
    if (functionRemoteSteps.containsKey(colName)) {
      RExecutionPlanStep previousStep = Iterables.getLast(functionRemoteSteps.get(colName));
      remoteWireManager.wire(ColumnBuiltConsumer.class, previousStep, targetStep);
    }
  }

  @Override
  public void wireGroupInput(RExecutionPlanStep groupStep) {
    // wire group step as input to all Group intermediate Aggregate functions - they need the grouping information to
    // execute on.
    functionRemoteSteps.values().stream().flatMap(v -> v.stream()).forEach(new Consumer<RExecutionPlanStep>() {
      @Override
      public void accept(RExecutionPlanStep step) {
        if (step.getType().equals(RExecutionPlanStepType.GROUP_INTERMEDIATE_AGGREGATE))
          remoteWireManager.wire(GroupDeltaConsumer.class, groupStep, step);
      }
    });
  }

  @Override
  public void prepareBuild() {
    // noop
  }

  @Override
  public List<RExecutionPlanStep> build() {
    // columnValuesProvidingStep is ignored.
    for (Entry<String, List<RExecutionPlanStep>> remoteEntry : functionRemoteSteps.entrySet()) {
      PlannerColumnInfo colInfo = columnInfo.get(remoteEntry.getKey());
      RExecutionPlanStep inputStep = Iterables.getFirst(remoteEntry.getValue(), null);
      for (String prevColumnName : colInfo.getDependsOnColumns())
        wireOutputOfColumnIfAvailable(prevColumnName, inputStep);
    }

    List<RExecutionPlanStep> allSteps =
        functionRemoteSteps.values().stream().flatMap(lst -> lst.stream()).collect(Collectors.toList());
    return allSteps;
  }

  @Override
  public boolean isColumnProduced(String colName) {
    return functionRemoteSteps.containsKey(colName);
  }
}
