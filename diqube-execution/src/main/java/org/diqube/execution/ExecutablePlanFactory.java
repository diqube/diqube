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
package org.diqube.execution;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.diqube.cluster.ClusterLayout;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnShardFactory;
import org.diqube.execution.steps.BuildColumnFromValuesStep;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.execution.steps.FilterRequestedColumnsAndActiveRowIdsStep;
import org.diqube.execution.steps.FlattenStep;
import org.diqube.execution.steps.GroupFinalAggregationStep;
import org.diqube.execution.steps.GroupIdAdjustingStep;
import org.diqube.execution.steps.HavingResultStep;
import org.diqube.execution.steps.OrderStep;
import org.diqube.execution.steps.OverwritingRowIdAndStep;
import org.diqube.execution.steps.OverwritingRowIdNotStep;
import org.diqube.execution.steps.OverwritingRowIdOrStep;
import org.diqube.execution.steps.ProjectStep;
import org.diqube.execution.steps.ResolveColumnDictIdsStep;
import org.diqube.execution.steps.ResolveValuesStep;
import org.diqube.execution.steps.RowIdEqualsStep;
import org.diqube.execution.steps.RowIdInequalStep;
import org.diqube.execution.steps.RowIdInequalStep.RowIdComparator;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.flatten.QueryMasterFlattenService;
import org.diqube.function.FunctionFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.queries.QueryRegistry;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;

/**
 * Factory for {@link ExecutablePlanStep}s and {@link ExecutablePlan}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ExecutablePlanFactory {
  @Inject
  private FunctionFactory functionFactory;

  @Inject
  private ColumnShardBuilderFactory columnShardBuilderFactory;

  @Inject
  private ColumnShardFactory columnShardFactory;

  @Inject
  private QueryRegistry queryRegistry;

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private QueryMasterFlattenService queryMasterFlattenService;

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  public GroupFinalAggregationStep createGroupFinalAggregationStep(int stepId, ExecutionEnvironment env,
      String functionNameLowerCase, String outputColName, ColumnVersionManager columnVersionManager,
      List<Object> constantFunctionParameters) {
    return new GroupFinalAggregationStep(stepId, queryRegistry, env, functionFactory, columnShardBuilderFactory,
        functionNameLowerCase, outputColName, columnVersionManager, constantFunctionParameters);
  }

  public ProjectStep createProjectStep(int stepId, ExecutionEnvironment env, String functionNameLowerCase,
      String outputColName, ColumnOrValue[] functionParameters, ColumnVersionManager columnVersionManager) {
    return new ProjectStep(stepId, queryRegistry, env, functionFactory, functionNameLowerCase, functionParameters,
        outputColName, columnShardBuilderFactory, columnShardFactory, columnVersionManager);
  }

  public ResolveColumnDictIdsStep createResolveColumnDictIdStep(int stepId, ExecutionEnvironment env, String colName) {
    return new ResolveColumnDictIdsStep(stepId, queryRegistry, env, colName);
  }

  public ResolveValuesStep createResolveValuesStep(int stepId) {
    return new ResolveValuesStep(stepId, queryRegistry);
  }

  public ExecutablePlan createExecutablePlan(ExecutionEnvironment defaultEnv, List<ExecutablePlanStep> steps,
      ExecutablePlanInfo info, ColumnVersionManager columnVersionManager) {
    return new ExecutablePlan(defaultEnv, steps, info, columnVersionManager);
  }

  public ExecutablePlanInfo createExecutablePlanInfo(List<String> selectedColumnNames, List<String> selectionRequests,
      boolean isOrdered, boolean isGrouped, boolean having) {
    return new ExecutablePlanInfo(selectedColumnNames, selectionRequests, isOrdered, isGrouped, having);
  }

  public ExecuteRemotePlanOnShardsStep createExecuteRemotePlanStep(int stepId, ExecutionEnvironment env,
      RExecutionPlan remotePlan) {
    return new ExecuteRemotePlanOnShardsStep(stepId, queryRegistry, env, remotePlan, clusterLayout,
        connectionOrLocalHelper, ourNodeAddressProvider);
  }

  public OrderStep createOrderStep(int stepId, ExecutionEnvironment env, List<Pair<String, Boolean>> sortCols,
      Long limit, Long limitStart, Long softLimit) {
    return new OrderStep(stepId, queryRegistry, env, sortCols, limit, limitStart, softLimit);
  }

  public BuildColumnFromValuesStep createBuildColumnFromValuesStep(int stepId, ExecutionEnvironment env, String colName,
      ColumnVersionManager columnVersionManager) {
    return new BuildColumnFromValuesStep(stepId, queryRegistry, env, colName, columnShardBuilderFactory,
        columnVersionManager);
  }

  public FilterRequestedColumnsAndActiveRowIdsStep createFilterRequestedColumnsValuesStep(int stepId,
      Set<String> requestedColNames) {
    return new FilterRequestedColumnsAndActiveRowIdsStep(stepId, queryRegistry, requestedColNames);
  }

  public GroupIdAdjustingStep createGroupIdAdjustingStep(int stepId, Set<String> groupedColNames) {
    return new GroupIdAdjustingStep(stepId, queryRegistry, groupedColNames);
  }

  public RowIdEqualsStep createRowIdEqualsStep(int stepId, ExecutionEnvironment env, String colName,
      Object[] sortedValues) {
    return new RowIdEqualsStep(stepId, queryRegistry, env, colName, sortedValues);
  }

  public RowIdEqualsStep createRowIdEqualsStep(int stepId, ExecutionEnvironment env, String colName,
      String otherColName) {
    return new RowIdEqualsStep(stepId, queryRegistry, env, colName, otherColName);
  }

  public RowIdInequalStep createRowIdInequalStep(int stepId, ExecutionEnvironment env, String colName, Object value,
      RowIdComparator comparator) {
    return new RowIdInequalStep(stepId, queryRegistry, env, colName, value, comparator);
  }

  public RowIdInequalStep createRowIdInequalStep2Cols(int stepId, ExecutionEnvironment env, String colName,
      String otherColName, RowIdComparator comparator) {
    return new RowIdInequalStep(stepId, queryRegistry, env, colName, otherColName, comparator, true);
  }

  public OverwritingRowIdAndStep createOverwritingRowIdAndStep(int stepId) {
    return new OverwritingRowIdAndStep(stepId, queryRegistry);
  }

  public OverwritingRowIdOrStep createOverwritingRowIdOrStep(int stepId) {
    return new OverwritingRowIdOrStep(stepId, queryRegistry);
  }

  public OverwritingRowIdNotStep createOverwritingRowIdNotStep(int stepId) {
    return new OverwritingRowIdNotStep(stepId, queryRegistry);
  }

  public HavingResultStep createHavingResultStep(int stepId) {
    return new HavingResultStep(stepId, queryRegistry);
  }

  public FlattenStep createFlattenStep(int stepId, String tableName, String flattenBy) {
    return new FlattenStep(stepId, queryRegistry, tableName, flattenBy, queryMasterFlattenService);
  }

}
