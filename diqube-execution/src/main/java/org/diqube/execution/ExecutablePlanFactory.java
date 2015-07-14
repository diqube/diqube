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

import org.diqube.context.AutoInstatiate;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.steps.BuildColumnFromValuesStep;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.execution.steps.FilterRequestedColumnsAndActiveRowIdsStep;
import org.diqube.execution.steps.GroupFinalAggregationStep;
import org.diqube.execution.steps.GroupIdAdjustingStep;
import org.diqube.execution.steps.OrderStep;
import org.diqube.execution.steps.ProjectStep;
import org.diqube.execution.steps.ResolveColumnDictIdsStep;
import org.diqube.execution.steps.ResolveValuesStep;
import org.diqube.function.FunctionFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.threads.ExecutorManager;
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
  private ExecutablePlanFromRemoteBuilderFactory executablePlanFromRemoteBuilderFactory;

  @Inject
  private ColumnShardFactory columnShardFactory;

  @Inject
  private ExecutorManager executorManager;

  public GroupFinalAggregationStep createGroupFinalAggregationStep(int stepId, ExecutionEnvironment env,
      String functionNameLowerCase, String outputColName, ColumnVersionManager columnVersionManager) {
    return new GroupFinalAggregationStep(stepId, env, functionFactory, columnShardBuilderFactory, functionNameLowerCase,
        outputColName, columnVersionManager);
  }

  public ProjectStep createProjectStep(int stepId, ExecutionEnvironment env, String functionNameLowerCase,
      String outputColName, ColumnOrValue[] functionParameters, ColumnVersionManager columnVersionManager) {
    return new ProjectStep(stepId, env, functionFactory, functionNameLowerCase, functionParameters, outputColName,
        columnShardBuilderFactory, columnShardFactory, columnVersionManager);
  }

  public ResolveColumnDictIdsStep createResolveColumnDictIdStep(int stepId, ExecutionEnvironment env, String colName) {
    return new ResolveColumnDictIdsStep(stepId, env, colName);
  }

  public ResolveValuesStep createResolveValuesStep(int stepId) {
    return new ResolveValuesStep(stepId);
  }

  public ExecutablePlan createExecutablePlan(ExecutionEnvironment defaultEnv, List<ExecutablePlanStep> steps,
      ExecutablePlanInfo info) {
    return new ExecutablePlan(defaultEnv, steps, info);
  }

  public ExecutablePlanInfo createExecutablePlanInfo(List<String> selectedColumnNames, boolean isOrdered,
      boolean isGrouped) {
    return new ExecutablePlanInfo(selectedColumnNames, isOrdered, isGrouped);
  }

  public ExecuteRemotePlanOnShardsStep createExecuteRemotePlanStep(int stepId, ExecutionEnvironment env,
      RExecutionPlan remotePlan) {
    return new ExecuteRemotePlanOnShardsStep(stepId, env, remotePlan, executablePlanFromRemoteBuilderFactory,
        executorManager);
  }

  public OrderStep createOrderStep(int stepId, ExecutionEnvironment env, List<Pair<String, Boolean>> sortCols,
      Long limit, Long limitStart, Long softLimit) {
    return new OrderStep(stepId, env, sortCols, limit, limitStart, softLimit);
  }

  public BuildColumnFromValuesStep createBuildColumnFromValuesStep(int stepId, ExecutionEnvironment env, String colName,
      ColumnVersionManager columnVersionManager) {
    return new BuildColumnFromValuesStep(stepId, env, colName, columnShardBuilderFactory, columnVersionManager);
  }

  public FilterRequestedColumnsAndActiveRowIdsStep createFilterRequestedColumnsValuesStep(int stepId,
      Set<String> requestedColNames) {
    return new FilterRequestedColumnsAndActiveRowIdsStep(stepId, requestedColNames);
  }

  public GroupIdAdjustingStep createGroupIdAdjustingStep(int stepId, Set<String> groupedColNames) {
    return new GroupIdAdjustingStep(stepId, groupedColNames);
  }

}
