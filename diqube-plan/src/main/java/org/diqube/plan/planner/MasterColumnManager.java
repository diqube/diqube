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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.ColumnVersionManagerFactory;
import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.steps.BuildColumnFromValuesStep;
import org.diqube.execution.steps.GroupFinalAggregationStep;
import org.diqube.execution.steps.ProjectStep;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.util.ColumnOrValue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * {@link ColumnManager} for the Query Master node.
 *
 * @author Bastian Gloeckle
 */
public class MasterColumnManager implements ColumnManager<ExecutablePlanStep> {
  private Map<String, List<ExecutablePlanStep>> functionMasterSteps = new HashMap<>();
  private ExecutionEnvironment env;
  private Supplier<Integer> nextMasterStepIdSupplier;
  private ExecutablePlanFactory executablePlanFactory;
  private Map<String, PlannerColumnInfo> columnInfo;
  private Set<String> columnsThatNeedToBeAvailable = new HashSet<>();
  private Map<String, List<ExecutablePlanStep>> delayedWires = new HashMap<>();
  private RemoteResolveManager remoteResolveManager;
  private MasterWireManager masterWireManager;
  private ExecutablePlanStep columnValuesProvidingStep;
  private ColumnVersionManager columnVersionManager;

  /**
   * @param remoteResolveManager
   *          This {@link RemoteResolveManager} will be fed with those columns that need to be available on the master
   *          and therefore need to be resolved on the remotes. Please note the JavaDoc of {@link #prepareBuild()}.
   */
  public MasterColumnManager(ExecutionEnvironment masterExecutuionEnvironment,
      Supplier<Integer> nextMasterStepIdSupplier, ExecutablePlanFactory executablePlanFactory,
      ColumnVersionManagerFactory columnVersionManagerFactory, Map<String, PlannerColumnInfo> columnInfo,
      RemoteResolveManager remoteResolveManager, MasterWireManager masterWireManager) {
    this.env = masterExecutuionEnvironment;
    this.nextMasterStepIdSupplier = nextMasterStepIdSupplier;
    this.executablePlanFactory = executablePlanFactory;
    this.columnInfo = columnInfo;
    this.remoteResolveManager = remoteResolveManager;
    this.masterWireManager = masterWireManager;
    this.columnVersionManager = columnVersionManagerFactory.createColumnVersionManager(env);
  }

  @Override
  public void produceColumn(FunctionRequest fnReq) {
    if (fnReq.getType().equals(FunctionRequest.Type.PROJECTION)) {
      ProjectStep projectStep = executablePlanFactory.createProjectStep(nextMasterStepIdSupplier.get(), env,
          fnReq.getFunctionName(), fnReq.getOutputColumn(),
          fnReq.getInputParameters().toArray(new ColumnOrValue[fnReq.getInputParameters().size()]),
          columnVersionManager);

      // ensure all input columns are fully available on master
      for (ColumnOrValue input : fnReq.getInputParameters()) {
        if (input.getType().equals(ColumnOrValue.Type.COLUMN))
          ensureColumnAvailable(input.getColumnName());
      }

      functionMasterSteps.put(fnReq.getOutputColumn(),
          new ArrayList<>(Arrays.asList(new ExecutablePlanStep[] { projectStep })));
    } else {
      // TODO #28 do NOT calculate all Grouped results on query master, but distribute groups according to group hash
      // along all cluster nodes. That means that those clusternodes would fully process specific sets of groups and
      // they would need to have all the data needed for calculating those groups transferred to them. This though
      // would decrease the load on the query master heavily, especially if the results are ordered by grouped
      // columns early.
      GroupFinalAggregationStep finalStep = executablePlanFactory.createGroupFinalAggregationStep(
          nextMasterStepIdSupplier.get(), env, fnReq.getFunctionName(), fnReq.getOutputColumn(), columnVersionManager);

      functionMasterSteps.put(fnReq.getOutputColumn(),
          new ArrayList<>(Arrays.asList(new ExecutablePlanStep[] { finalStep })));
    }
  }

  @Override
  public void ensureColumnAvailable(String colName) {
    columnsThatNeedToBeAvailable.add(colName);
  }

  @Override
  public void wireOutputOfColumnIfAvailable(String colName, ExecutablePlanStep targetStep) {
    if (functionMasterSteps.containsKey(colName)) {
      ExecutablePlanStep previousStep = Iterables.getLast(functionMasterSteps.get(colName));
      masterWireManager.wire(ColumnVersionBuiltConsumer.class, previousStep, targetStep);
      masterWireManager.wire(ColumnBuiltConsumer.class, previousStep, targetStep);
    } else if (columnsThatNeedToBeAvailable.contains(colName)) {
      if (!delayedWires.containsKey(colName))
        delayedWires.put(colName, new ArrayList<>());
      delayedWires.get(colName).add(targetStep);
    }
  }

  /**
   * Accepts a step that provides a {@link GroupIntermediaryAggregationConsumer} - these intermediate results are
   * provided by cluster nodes and need to be finalized on the query master.
   */
  @Override
  public void wireGroupInput(ExecutablePlanStep groupIntermediateAggregateSourceStep) {
    // wire GroupFinalAggregationSteps to a source of row IDs.
    functionMasterSteps.values().stream().flatMap(steps -> steps.stream())
        .filter(step -> step instanceof GroupFinalAggregationStep).forEach(new Consumer<ExecutablePlanStep>() {
          @Override
          public void accept(ExecutablePlanStep groupFinalAggStep) {
            masterWireManager.wire(GroupIntermediaryAggregationConsumer.class, groupIntermediateAggregateSourceStep,
                groupFinalAggStep);
          }
        });
  }

  /**
   * Prepares the call to {@link #build()}. Execute before building the steps of the {@link RemoteResolveManager} that
   * was specified in the constructor: This method will add any needed resolve steps in that
   * {@link RemoteResolveManager} if the query master needs additional columns to be resolved!
   */
  @Override
  public void prepareBuild() {
    // ensure the source columns of the columns that are calculated on the query master are available
    // Aggregation columns do not need the whole columns on query master, as they will receive the groupIntermediary
    // results from the cluster nodes
    for (Entry<String, List<ExecutablePlanStep>> remoteEntry : functionMasterSteps.entrySet()) {
      PlannerColumnInfo colInfo = columnInfo.get(remoteEntry.getKey());
      if (colInfo != null && !colInfo.getType().equals(FunctionRequest.Type.AGGREGATION))
        for (String prevColumnName : colInfo.getDependsOnColumns())
          ensureColumnAvailable(prevColumnName);
    }

    // take care of the columns we need to ensure are available on the query master.
    for (String transferColName : Sets.difference(columnsThatNeedToBeAvailable, functionMasterSteps.keySet())) {
      ExecutablePlanStep masterCreationStep = executablePlanFactory
          .createBuildColumnFromValuesStep(nextMasterStepIdSupplier.get(), env, transferColName, columnVersionManager);
      functionMasterSteps.put(transferColName,
          new ArrayList<ExecutablePlanStep>(Arrays.asList(new ExecutablePlanStep[] { masterCreationStep })));

      // ensure the remote provides values for that column
      remoteResolveManager.resolveValuesOfColumn(transferColName);
    }
  }

  /**
   * @param columnValuesProvidingStep
   *          The {@link ExecutablePlanStep} that provides all values of all columns that the cluster nodes have
   *          resolved. This is needed for those columns that need to be created on the query master (=
   *          {@link #ensureColumnAvailable(String)} was called).
   */
  public void provideColumnValuesProvidingStep(ExecutablePlanStep columnValuesProvidingStep) {
    this.columnValuesProvidingStep = columnValuesProvidingStep;
  }

  /**
   * Call {@link #provideColumnValuesProvidingStep(ExecutablePlanStep)} before this!
   */
  @Override
  public List<ExecutablePlanStep> build() {
    if (columnValuesProvidingStep == null)
      throw new IllegalStateException("Column values step was not provided.");

    // wire the columns that have been created; the source columns are available on the query master already, see
    // #prepareBuild.
    // Aggregation columns do not need to wait for column builts of parameters on query master, as they will receive the
    // groupIntermediary results from the cluster nodes and will not work on the column values directly.
    for (Entry<String, List<ExecutablePlanStep>> masterEntry : functionMasterSteps.entrySet()) {
      PlannerColumnInfo colInfo = columnInfo.get(masterEntry.getKey());
      if (colInfo != null && !colInfo.getType().equals(FunctionRequest.Type.AGGREGATION)) {
        ExecutablePlanStep inputStep = Iterables.getFirst(masterEntry.getValue(), null);
        for (String prevColumnName : colInfo.getDependsOnColumns())
          wireOutputOfColumnIfAvailable(prevColumnName, inputStep);
      }
    }

    // wire the BuildColumnFromValues steps inputs to the step that is providing the colum values.
    functionMasterSteps.values().stream().flatMap(lst -> lst.stream())
        .filter(step -> step instanceof BuildColumnFromValuesStep).forEach(new Consumer<ExecutablePlanStep>() {
          @Override
          public void accept(ExecutablePlanStep buildColumnFromValuesStep) {
            masterWireManager.wire(ColumnValueConsumer.class, columnValuesProvidingStep, buildColumnFromValuesStep);
          }
        });

    // execute delayedWires, as now all columns should be represented in functionMasterSteps.
    for (String sourceColName : delayedWires.keySet())
      for (ExecutablePlanStep targetStep : delayedWires.get(sourceColName))
        wireOutputOfColumnIfAvailable(sourceColName, targetStep);

    List<ExecutablePlanStep> allSteps =
        functionMasterSteps.values().stream().flatMap(lst -> lst.stream()).collect(Collectors.toList());
    return allSteps;
  }

  @Override
  public boolean isColumnProduced(String colName) {
    return functionMasterSteps.containsKey(colName);
  }
}
