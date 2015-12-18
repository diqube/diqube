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

import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.request.FunctionRequest;
import org.diqube.diql.request.FunctionRequest.Type;
import org.diqube.diql.request.ResolveValueRequest;
import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.ColumnVersionManagerFactory;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanInfo;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.consumers.TableFlattenedConsumer;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.execution.steps.FlattenStep;
import org.diqube.execution.steps.HavingResultStep;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.name.FlattenedTableNameGenerator;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.PlannerColumnInfoBuilder;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.plan.exception.PlanBuildException;
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

  private ColumnVersionManagerFactory columnVersionManagerFactory;

  private FlattenedTableNameGenerator flattenedTableNameGenerator;

  private int nextMasterStepId = 0;
  private int nextRemoteStepId = 0;

  public ExecutionPlanner(ExecutablePlanFactory executablePlanFactory,
      RemoteExecutionPlanFactory remoteExecutionPlanFactory, ColumnVersionManagerFactory columnVersionManagerFactory,
      FlattenedTableNameGenerator flattenedTableNameGenerator) {
    this.executablePlanFactory = executablePlanFactory;
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.columnVersionManagerFactory = columnVersionManagerFactory;
    this.flattenedTableNameGenerator = flattenedTableNameGenerator;
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

    // TODO #19 support selecting constants
    Set<String> resultColNamesRequested = executionRequest.getResolveValues().stream()
        .map(resolveReq -> resolveReq.getResolve().getColumnName()).collect(Collectors.toSet());

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

    ColumnVersionManager masterColumnVersionManager =
        columnVersionManagerFactory.createColumnVersionManager(masterDefaultExecutionEnv);

    MasterColumnManager masterColManager = new MasterColumnManager(masterDefaultExecutionEnv, nextMasterIdSupplier,
        executablePlanFactory, masterColumnVersionManager, columnInfo, remoteResolveManager, masterWireManager);
    MasterResolveManager masterResolveManager = new MasterResolveManager(nextMasterIdSupplier,
        masterDefaultExecutionEnv, executablePlanFactory, masterColManager, masterWireManager, resultColNamesRequested);

    // ==== Take care of columns that need to be created (e.g. by projection and aggregation) and feed this info into
    // column managers

    Set<String> columnNamesWorkedOn = new HashSet<>();
    for (FunctionRequest fnReq : executionRequest.getProjectAndAggregate()) {
      if (columnNamesWorkedOn.contains(fnReq.getOutputColumn()))
        // remember: we want to calculate the same function with the same arguments only once. See class comment of
        // FunctionRequest.
        continue;
      columnNamesWorkedOn.add(fnReq.getOutputColumn());

      if (columnInfo.get(fnReq.getOutputColumn()).isTransitivelyDependsOnRowAggregation()) {
        // the resulting col depends on a row that will be created by a row aggregation (GROUP BY). As the final values
        // of the group by is available only on the query master, we need to produce that new column only there, too.

        // Not that the validator made sure that this new Fn is not again a row Agg function or a col Agg function.

        masterColManager.produceColumn(fnReq);
      } else {
        if (fnReq.getType().equals(Type.AGGREGATION_ROW)) {
          // A row aggregation (GROUP BY aggregation). Both, remote and master need to do something:
          // Intermediate part of aggregation will be executed on cluster nodes, final part will be executed on query
          // master.
          remoteColManager.produceColumn(fnReq);
          masterColManager.produceColumn(fnReq);
        } else {
          // simple (REPEATED_)PROJECTION that is not based on a row Agg, or a column aggregation - all of these can be
          // executed on the remotes.
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

      for (String groupByCol : executionRequest.getGroup().getGroupColumns()) {
        // (1) make sure the columnBuiltConsumer is wired to the group step so the latter does not start too early
        remoteColManager.wireOutputOfColumnIfAvailable(groupByCol, groupStep);

        // (2) make sure that the values of the group by columns are sent to the master - these are needed for the
        // group ID adjustment step! We do not need to build a column for that (the GroupIdAdjust step just consumes the
        // values), but we definitely need to send the values!
        remoteResolveManager.resolveValuesOfColumn(groupByCol);
      }

      // group step consumes rowIDs provided by previous RowID consumer and provides the RowIDs for every future step
      // (because it will drastically reduce number of rowIds, because it merges multiple rows into groups -> groupIds
      // are rowIds).
      remoteWireManager.wire(RowIdConsumer.class, remoteRowIdSourceStep, groupStep);
      remoteRowIdSourceStep = groupStep;
      allRemoteSteps.add(groupStep);

      // the group step has to provide its data to all Group Intermediary aggregation steps.
      remoteColManager.wireGroupInput(groupStep);

      // TODO #24 we should make sure that results form GroupIntermediateAggregate steps are piped through an order step
      // in order to do a row-id cut-off.
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

      MasterOrderHandler masterOrderHandler = new MasterOrderHandler(executablePlanFactory, nextMasterIdSupplier,
          masterDefaultExecutionEnv, masterColManager);
      ExecutablePlanStep masterOrderStep = masterOrderHandler.build(executionRequest.getOrder());
      masterRowIdSourceStep = masterOrderStep;
      masterRowIdStartStep = masterOrderStep;
      allMasterSteps.add(masterOrderStep);
    }

    // ==== Make sure the values of the requested columns are resolved so they can be provided to the user.
    for (ResolveValueRequest resolveValue : executionRequest.getResolveValues()) {
      // TODO #19 support resolving constants
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
    TopologicalSort<RExecutionPlanStep> remoteTopSort = new TopologicalSort<RExecutionPlanStep>( //
        step -> {
          if (step.getProvideDataForSteps() != null) {
            return step.getProvideDataForSteps().keySet().stream().map(idx -> idToRemoteSteps.get(idx))
                .collect(Collectors.toList());
          }
          return new ArrayList<>();
        } , //
        step -> (long) step.getStepId(), //
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
        remoteExecutionPlanFactory.createExecutionPlan(allRemoteSteps, executionRequest.getFromRequest());

    // ==== Build execution plan for master node.

    // If flattened, be sure to trigger flattening correctly.
    FlattenStep flattenStep = null;
    if (executionRequest.getFromRequest().isFlattened()) {
      flattenStep = executablePlanFactory.createFlattenStep(nextMasterIdSupplier.get(),
          executionRequest.getFromRequest().getTable(), executionRequest.getFromRequest().getFlattenByField());
      allMasterSteps.add(flattenStep);
    }

    // Make query master execute remote execution plan on remotes.
    ExecutablePlanStep executeRemoteStep = executablePlanFactory.createExecuteRemotePlanStep(nextMasterIdSupplier.get(),
        masterDefaultExecutionEnv, remoteExecutionPlan);
    allMasterSteps.add(executeRemoteStep);

    if (flattenStep != null)
      masterWireManager.wire(TableFlattenedConsumer.class, flattenStep, executeRemoteStep);

    // ==== Handle a GROUP and aggregation functions on master
    boolean rowAggregateFunctionsAvailable = columnInfo.values().stream()
        .anyMatch(colInfo -> colInfo.getType().equals(FunctionRequest.Type.AGGREGATION_ROW));
    if (executionRequest.getGroup() != null && rowAggregateFunctionsAvailable) {
      // TODO #37: Query is probably wrong if it has "group" but no aggregation_row funcs. Inform user.

      // We are grouping and executing aggregation functions, that means that cluster nodes will reply with group
      // intermediary updates to the query master.
      // The groupIds used by the cluster nodes though are the row IDs of one of the rows contained in a group - which
      // means they are only valid locally to a cluster node: The same group (= the same values for the grouped columns)
      // will end up having different groupIds on each cluster node. We add this groupid adjusting step on the query
      // master to merge the groupIds.

      ExecutablePlanStep groupIdAdjustStep = executablePlanFactory.createGroupIdAdjustingStep(
          nextMasterIdSupplier.get(), new HashSet<>(executionRequest.getGroup().getGroupColumns()));
      // wire twice because the adjust step needs both, group Id intermediate information and column values from the
      // executeRemoteStep.
      masterWireManager.wire(ColumnValueConsumer.class, executeRemoteStep, groupIdAdjustStep);
      masterWireManager.wire(GroupIntermediaryAggregationConsumer.class, executeRemoteStep, groupIdAdjustStep);
      allMasterSteps.add(groupIdAdjustStep);

      // After merging the groupIds, that step will provide the updates to any group aggregation finalization functions
      // on the query master.
      masterColManager.wireGroupInput(groupIdAdjustStep);

      if (executionRequest.getHaving() != null) {
        HavingBuilder havingBuilder = new HavingBuilder(executablePlanFactory, nextMasterIdSupplier, masterColManager,
            masterDefaultExecutionEnv, masterWireManager);

        Pair<ExecutablePlanStep, List<ExecutablePlanStep>> p = havingBuilder.build(executionRequest.getHaving());

        HavingResultStep havingResultStep = executablePlanFactory.createHavingResultStep(nextMasterIdSupplier.get());

        masterWireManager.wire(OverwritingRowIdConsumer.class, p.getLeft(), havingResultStep);

        allMasterSteps.addAll(p.getRight());
        allMasterSteps.add(havingResultStep);
      }

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
    TopologicalSort<ExecutablePlanStep> masterTopSort = new TopologicalSort<ExecutablePlanStep>( //
        step -> {
          if (masterWires.containsKey(step.getStepId()))
            return masterWires.get(step.getStepId()).stream().map(idx -> idToMasterSteps.get(idx))
                .collect(Collectors.toList());
          return new ArrayList<>();
        } , //
        step -> (long) step.getStepId(), //
        (step, newIdx) -> step.setStepId(newIdx));
    allMasterSteps = masterTopSort.sort(allMasterSteps);
    // masterWires is invalid now!

    // TODO #19 support selecting non-cols
    ExecutablePlanInfo info = createInfo(executionRequest);
    ExecutablePlan plan = executablePlanFactory.createExecutablePlan(masterDefaultExecutionEnv, allMasterSteps, info,
        masterColumnVersionManager);

    return plan;
  }

  private ExecutablePlanInfo createInfo(ExecutionRequest executionRequest) {
    List<String> selectedCols = executionRequest.getResolveValues().stream()
        .map(res -> res.getResolve().getColumnName()).collect(Collectors.toList());
    List<String> selectionRequests =
        executionRequest.getResolveValues().stream().map(res -> res.getRequestString()).collect(Collectors.toList());

    boolean isOrdered = executionRequest.getOrder() != null;
    boolean isGrouped = executionRequest.getGroup() != null;
    boolean having = executionRequest.getHaving() != null;

    return executablePlanFactory.createExecutablePlanInfo(selectedCols, selectionRequests, isOrdered, isGrouped,
        having);
  }
}
