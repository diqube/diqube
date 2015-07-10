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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.diqube.execution.ColumnVersionManagerFactory;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanInfo;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.PlannerColumnInfoBuilder;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.plan.exception.PlanBuildException;
import org.diqube.plan.request.ExecutionRequest;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.plan.request.FunctionRequest.Type;
import org.diqube.plan.request.ResolveValueRequest;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDataType;
import org.diqube.util.Pair;
import org.diqube.util.TopologicalSort;

/**
 * Uses a {@link ExecutionRequest} to build an {@link ExecutablePlan} that is executable on the query master node. That
 * {@link ExecutablePlan} though will contain a {@link ExecuteRemotePlanOnShardsStep} which in turn will execute parts
 * of the overall-plan on the other cluster nodes.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionPlanner {

  private ExecutablePlanFactory executablePlanFactory;

  private RemoteExecutionPlanFactory remoteExecutionPlanFactory;

  private int nextMasterStepId = 0;
  private int nextRemoteStepId = 0;

  private ColumnVersionManagerFactory columnVersionManagerFactory;

  public ExecutionPlanner(ExecutablePlanFactory executablePlanFactory,
      RemoteExecutionPlanFactory remoteExecutionPlanFactory, ColumnVersionManagerFactory columnVersionManagerFactory) {
    this.executablePlanFactory = executablePlanFactory;
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.columnVersionManagerFactory = columnVersionManagerFactory;
  }

  /**
   * Executes planning.
   * 
   * @param executionRequest
   *          The input {@link ExecutionRequest} that was parsed from an diql stmt.
   * @param columnInfo
   *          The columnInfo for all {@link FunctionRequest} columns in the {@link ExecutionRequest}, see
   *          {@link PlannerColumnInfoBuilder}.
   * @param masterDefaultExecutionEnv
   *          The {@link ExecutionEnvironment} to be used for those {@link ExecutablePlanStep}s that will be run on the
   *          query master directly.
   * @return An {@link ExecutablePlan} that can be executed by the Query Master right away and which will distribute
   *         some workload to the other cluster node.
   */
  public ExecutablePlan plan(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> columnInfo,
      ExecutionEnvironment masterDefaultExecutionEnv) throws PlanBuildException {
    // ==== Initialize all helper objects

    // TODO support selecting constants
    Set<String> resultColNamesRequested =
        executionRequest.getResolveValues().stream().map(resolveReq -> resolveReq.getResolve().getColumnName())
            .collect(Collectors.toSet());

    MasterWireManager masterWireManager = new MasterWireManager();
    RemoteWireManager remoteWireManager = new RemoteWireManager();

    List<RExecutionPlanStep> allRemoteSteps = new ArrayList<>();
    List<ExecutablePlanStep> allMasterSteps = new ArrayList<>();

    Supplier<Integer> nextMasterIdSupplier = () -> nextMasterStepId++;
    Supplier<Integer> nextRemoteIdSupplier = () -> nextRemoteStepId++;

    RemoteColumnManager remoteColManager =
        new RemoteColumnManager(nextRemoteIdSupplier, remoteExecutionPlanFactory, columnInfo, remoteWireManager);
    RemoteResolveManager remoteResolveManager =
        new RemoteResolveManager(nextRemoteIdSupplier, remoteColManager, remoteExecutionPlanFactory, remoteWireManager);

    MasterColumnManager masterColManager =
        new MasterColumnManager(masterDefaultExecutionEnv, nextMasterIdSupplier, executablePlanFactory,
            columnVersionManagerFactory, columnInfo, remoteResolveManager, masterWireManager);
    MasterResolveManager masterResolveManager =
        new MasterResolveManager(nextMasterIdSupplier, masterDefaultExecutionEnv, executablePlanFactory, masterColManager,
            masterWireManager, resultColNamesRequested);

    // ==== Take care of columns that need to be created (e.g. by projection and aggregation) and feed this info into
    // column managers

    Set<String> columnNamesWorkedOn = new HashSet<>();
    for (FunctionRequest fnReq : executionRequest.getProjectAndAggregate()) {
      if (columnNamesWorkedOn.contains(fnReq.getOutputColumn()))
        // remember: we want to calculate the same function with the same arguments only once. See class comment of
        // FunctionRequest.
        continue;
      columnNamesWorkedOn.add(fnReq.getOutputColumn());

      if (columnInfo.get(fnReq.getOutputColumn()).isTransitivelyDependsOnAggregation()) {
        if (fnReq.getType().equals(Type.AGGREGATION))
          // Validator should break execution before this point already.
          throw new PlanBuildException("Aggregation on already aggregated value.");

        masterColManager.produceColumn(fnReq);
      } else {
        if (fnReq.getType().equals(Type.AGGREGATION)) {
          // Intermediate part of aggregation will be executed on cluster nodes, final part will be executed on query
          // master.
          remoteColManager.produceColumn(fnReq);
          masterColManager.produceColumn(fnReq);
        } else {
          remoteColManager.produceColumn(fnReq);
        }
      }
    }

    // The RowIdSink step will consume the results (=rowIDs) of the WHERE stmts if there are any. This step outputs the
    // rowIDs that other steps later on can rely on. If no input rowIds are provided, this will simply return /all/
    // rowIds (= no WHERE stmt).
    RExecutionPlanStep remoteRowSinkStep = remoteExecutionPlanFactory.createRowIdSink(nextRemoteIdSupplier.get());
    allRemoteSteps.add(remoteRowSinkStep);

    // ==== Create remote steps for WHERE clause
    if (executionRequest.getWhere() != null) {
      WhereBuilder whereHandler =
          new WhereBuilder(remoteExecutionPlanFactory, nextRemoteIdSupplier, remoteColManager, remoteWireManager);
      Pair<RExecutionPlanStep, List<RExecutionPlanStep>> whereResult = whereHandler.build(executionRequest.getWhere());

      // let the WHERE steps provide the Row IDs to the RowIdSink.
      remoteWireManager.wire(RowIdConsumer.class, whereResult.getLeft(), remoteRowSinkStep);

      allRemoteSteps.addAll(whereResult.getRight());
    }

    // Row Sink provides rowIDs to work on
    RExecutionPlanStep remoteRowIdSourceStep = remoteRowSinkStep;

    // ==== Create remote steps for a potential GROUP clause (master steps will be created below).
    if (executionRequest.getGroup() != null) {
      RExecutionPlanStep groupStep =
          remoteExecutionPlanFactory.createGroup(executionRequest.getGroup(), nextRemoteIdSupplier.get());
      // group step consumes rowIDs provided by previous RowID consumer and provides the RowIDs for every future step
      // (because it will drastically reduce number of rowIds, because it merges multiple rows into groups -> groupIds
      // are rowIds).
      remoteWireManager.wire(RowIdConsumer.class, remoteRowIdSourceStep, groupStep);
      remoteRowIdSourceStep = groupStep;
      allRemoteSteps.add(groupStep);

      // the group step has to provide its data to all Group Intermediary aggregation steps.
      remoteColManager.wireGroupInput(groupStep);

      // TODO we should make sure that results form GroupIntermediateAggregate steps are piped through an order step in
      // order to do a row-id cut-off.

      // TODO add HAVING
    }

    ExecutablePlanStep masterRowIdSourceStep = null;
    ExecutablePlanStep masterRowIdStartStep = null;

    // ==== Feed information about ORDER into both, remote- and master- OrderHandler.
    if (executionRequest.getOrder() != null) {
      RemoteOrderHandler remoteOrderHandler =
          new RemoteOrderHandler(columnInfo, nextRemoteIdSupplier, remoteExecutionPlanFactory, remoteColManager);
      RExecutionPlanStep remoteOrderStep = remoteOrderHandler.build(executionRequest.getOrder());

      if (remoteOrderStep != null) {
        // order step consumes rowIDs provided by previous RowID consumer and provides the RowIDs for every future step,
        // because it might reduce number of rowIDs (because of a LIMIT clause).
        remoteWireManager.wire(RowIdConsumer.class, remoteRowIdSourceStep, remoteOrderStep);
        remoteRowIdSourceStep = remoteOrderStep;
        allRemoteSteps.add(remoteOrderStep);
      }

      MasterOrderHandler masterOrderHandler =
          new MasterOrderHandler(executablePlanFactory, nextMasterIdSupplier, masterDefaultExecutionEnv, masterColManager);
      ExecutablePlanStep masterOrderStep = masterOrderHandler.build(executionRequest.getOrder());
      masterRowIdSourceStep = masterOrderStep;
      masterRowIdStartStep = masterOrderStep;
      allMasterSteps.add(masterOrderStep);
    }

    // ==== Make sure the values of the requested columns are resolved so they can be provided to the user.
    for (ResolveValueRequest resolveValue : executionRequest.getResolveValues()) {
      // TODO support resolving constants
      String colName = resolveValue.getResolve().getColumnName();

      if (masterColManager.isColumnProduced(colName))
        masterResolveManager.resolveValuesOfColumn(colName);
      else
        // If the values are not produced on the master (=aggregations on master or projections thereof), those values
        // are available on the remotes. Make sure that we resolve them there - they will then automatically be sent to
        // the master from the remote and the master will automatically pass them on to a
        // FilterRequestedColumnsAndActiveRowIdsStep (which in turn will hand them over to the caller).
        remoteResolveManager.resolveValuesOfColumn(colName);
    }

    masterColManager.prepareBuild();

    // ==== Start building execution plan for remotes.
    allRemoteSteps.addAll(remoteResolveManager.build(remoteRowIdSourceStep));
    remoteColManager.prepareBuild();
    allRemoteSteps.addAll(remoteColManager.build());

    // do topological sort to make plans easier readable and support faster execution (because threads of first steps
    // will start to run first).
    Map<Integer, RExecutionPlanStep> idToRemoteSteps = new HashMap<>();
    for (RExecutionPlanStep remoteStep : allRemoteSteps)
      idToRemoteSteps.put(remoteStep.getStepId(), remoteStep);
    Map<Integer, Integer> remoteIdChangeMap = new HashMap<>();
    TopologicalSort<RExecutionPlanStep> remoteTopSort =
        new TopologicalSort<RExecutionPlanStep>( //
            step -> {
              if (step.getProvideDataForSteps() != null) {
                return step.getProvideDataForSteps().keySet().stream().map(idx -> idToRemoteSteps.get(idx))
                    .collect(Collectors.toList());
              }
              return new ArrayList<>();
            },//
            step -> (long) step.getStepId(),//
            (step, newIdx) -> remoteIdChangeMap.put(step.getStepId(), newIdx));
    allRemoteSteps = remoteTopSort.sort(allRemoteSteps);
    // Adjust Ids of steps according to top sort.
    for (RExecutionPlanStep remoteStep : allRemoteSteps) {
      if (remoteStep.getProvideDataForSteps() != null) {
        Map<Integer, List<RExecutionPlanStepDataType>> newProvideDataForSteps = new HashMap<>();
        for (Entry<Integer, List<RExecutionPlanStepDataType>> originalEntry : remoteStep.getProvideDataForSteps()
            .entrySet())
          newProvideDataForSteps.put(remoteIdChangeMap.get(originalEntry.getKey()), originalEntry.getValue());

        remoteStep.setProvideDataForSteps(newProvideDataForSteps);
      }
      remoteStep.setStepId(remoteIdChangeMap.get(remoteStep.getStepId()));
    }

    // Build remote execution plan
    RExecutionPlan remoteExecutionPlan =
        remoteExecutionPlanFactory.createExecutionPlan(allRemoteSteps, executionRequest.getTableName());

    // ==== Build execution plan for master node.
    // Make query master execute remote execution plan on remotes.
    ExecutablePlanStep executeRemoteStep =
        executablePlanFactory.createExecuteRemotePlanStep(nextMasterIdSupplier.get(), masterDefaultExecutionEnv,
            remoteExecutionPlan);
    allMasterSteps.add(executeRemoteStep);

    // ==== Handle a GROUP and aggregation functions on master
    boolean aggregateFunctionsAvailable =
        columnInfo.values().stream().anyMatch(colInfo -> colInfo.getType().equals(FunctionRequest.Type.AGGREGATION));
    if (executionRequest.getGroup() != null && aggregateFunctionsAvailable) {
      // We are grouping and executing aggregation functions, that means that cluster nodes will reply with group
      // intermediary updates to the query master.
      // The groupIds used by the cluster nodes though are the row IDs of one of the rows contained in a group - which
      // means they are only valid locally to a cluster node: The same group (= the same values for the grouped columns)
      // will end up having different groupIds on each cluster node. We add this groupid adjusting step on the query
      // master to merge the groupIds.

      ExecutablePlanStep groupIdAdjustStep =
          executablePlanFactory.createGroupIdAdjustingStep(nextMasterIdSupplier.get(), new HashSet<>(executionRequest
              .getGroup().getGroupColumns()));
      // wire twice because the adjust step needs both, group Id intermediate information and column values from the
      // executeRemoteStep.
      masterWireManager.wire(ColumnValueConsumer.class, executeRemoteStep, groupIdAdjustStep);
      masterWireManager.wire(GroupIntermediaryAggregationConsumer.class, executeRemoteStep, groupIdAdjustStep);
      allMasterSteps.add(groupIdAdjustStep);

      // After merging the groupIds, that step will provide the updates to any group aggregation finalization functions
      // on the query master.
      masterColManager.wireGroupInput(groupIdAdjustStep);

      if (masterRowIdStartStep != null) {
        masterWireManager.wire(RowIdConsumer.class, groupIdAdjustStep, masterRowIdStartStep);
        masterRowIdStartStep = groupIdAdjustStep;
      } else {
        masterRowIdStartStep = groupIdAdjustStep;
        masterRowIdSourceStep = groupIdAdjustStep;
      }
    } else if (masterRowIdStartStep != null)
      // we have a specific step that wants to consume all row IDs and is != to the group id adjust step (which does not
      // need a RowIdConsumer input), so wire it to the RowIdConsumer output of executeRemoteStep.
      masterWireManager.wire(RowIdConsumer.class, executeRemoteStep, masterRowIdStartStep);

    if (masterRowIdSourceStep == null)
      // we do not have a specific source of RowIDs for all resolving steps etc, just use all the row ids provided by
      // cluster nodes.
      masterRowIdSourceStep = executeRemoteStep;

    masterResolveManager.provideColumnValueSourceStep(executeRemoteStep);
    masterColManager.provideColumnValuesProvidingStep(executeRemoteStep);

    allMasterSteps.addAll(masterResolveManager.build(masterRowIdSourceStep));
    allMasterSteps.addAll(masterColManager.build());

    Map<Integer, Set<Integer>> masterWires = masterWireManager.buildFinalWireMap(allMasterSteps);

    // top sort masters plan, too
    Map<Integer, ExecutablePlanStep> idToMasterSteps = new HashMap<>();
    for (ExecutablePlanStep masterStep : allMasterSteps)
      idToMasterSteps.put(masterStep.getStepId(), masterStep);
    TopologicalSort<ExecutablePlanStep> masterTopSort =
        new TopologicalSort<ExecutablePlanStep>( //
            step -> {
              if (masterWires.containsKey(step.getStepId()))
                return masterWires.get(step.getStepId()).stream().map(idx -> idToMasterSteps.get(idx))
                    .collect(Collectors.toList());
              return new ArrayList<>();
            },//
            step -> (long) step.getStepId(),//
            (step, newIdx) -> step.setStepId(newIdx));
    allMasterSteps = masterTopSort.sort(allMasterSteps);
    // masterWires is invalid now!

    // TODO support selecting non-cols
    ExecutablePlanInfo info = createInfo(executionRequest);
    ExecutablePlan plan = executablePlanFactory.createExecutablePlan(masterDefaultExecutionEnv, allMasterSteps, info);

    return plan;
  }

  private ExecutablePlanInfo createInfo(ExecutionRequest executionRequest) {
    List<String> selectedCols =
        executionRequest.getResolveValues().stream().map(res -> res.getResolve().getColumnName())
            .collect(Collectors.toList());

    boolean isOrdered = executionRequest.getOrder() != null;
    boolean isGrouped = executionRequest.getGroup() != null;

    return executablePlanFactory.createExecutablePlanInfo(selectedCols, isOrdered, isGrouped);
  }
}
