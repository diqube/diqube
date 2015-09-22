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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.execution.cache.ColumnShardCache;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.remote.cluster.thrift.RColOrValue;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDataType;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Optimizes a {@link RExecutionPlan} that was received from a query master according to the circumstances a local
 * {@link TableShard} provides.
 *
 * @author Bastian Gloeckle
 */
public class ExecutablePlanFromRemoteOptimizer {
  private static final Logger logger = LoggerFactory.getLogger(ExecutablePlanFromRemoteOptimizer.class);

  /**
   * Optimizes the given plan to be executed on the given {@link ExecutionEnvironment}.
   * 
   * <p>
   * Note that when running this, {@link ColumnShard}s from the {@link ColumnShardCache} might already be put into the
   * provided {@link ExecutionEnvironment}.
   * 
   * <p>
   * This method must be executed with correct {@link QueryUuidThreadState} set, as it accesses the
   * {@link ExecutionEnvironment}.
   * 
   * @param defaultEnv
   *          The {@link ExecutionEnvironment} the resulting plan should be executed on. This is expected to be backed
   *          by a concrete {@link TableShard} (and probably a {@link ColumnShardCache}). These properties of these
   *          objects will be inspected for optimizing the plan - e.g. if a specific column is available in the cache
   *          already, we do not need to execute a ProjectStep that would create the same column, so that ProjectStep
   *          will be removed in the resulting executable plan.
   * @param plan
   *          The source plan as provided by the query master. That plan is basically a plan which we'd have to execute
   *          if there'd be no caches etc. We though are free to adjust that plan to the circumstances we find in the
   *          local {@link TableShard} and {@link ColumnShardCache} that we'll be executing on. Important is just that
   *          our plan creates the same output as the one that the query master sent.
   * @return A new {@link RExecutionPlan}, optimized to be executed on the given env.
   */
  public RExecutionPlan optimize(ExecutionEnvironment defaultEnv, RExecutionPlan plan) {
    RExecutionPlan res = new RExecutionPlan(plan);
    removeUnneededColumnCreations(defaultEnv, res);
    if (!res.equals(plan))
      logger.info("Optimized plan to {}", res.toString());
    return res;
  }

  /**
   * Checks all {@link RExecutionPlanStepType#PROJECT} and {@link RExecutionPlanStepType#COLUMN_AGGREGATE} steps and
   * identifies steps that do not need to be executed, because their result column exists already (e.g. in a cache). It
   * will then remove these steps and all steps that would be executed only for their results to be fed into the removed
   * steps (transitively).
   */
  private void removeUnneededColumnCreations(ExecutionEnvironment defaultEnv, RExecutionPlan plan) {
    Map<String, RExecutionPlanStep> columnCreatingSteps = new HashMap<>();
    Map<String, List<String>> sourceColumns = new HashMap<>();
    Map<String, Integer> numberOfFollowUpSteps = new HashMap<>();
    for (RExecutionPlanStep step : plan.getSteps()) {
      switch (step.getType()) {
      case COLUMN_AGGREGATE:
      case PROJECT:
      case REPEATED_PROJECT:
        String outCol = step.getDetailsFunction().getResultColumn().getColName();
        columnCreatingSteps.put(outCol, step);
        sourceColumns.put(outCol, new ArrayList<>());
        int provideDataForStepsColBuiltCount = step.getProvideDataForSteps().entrySet().stream()
            .mapToInt(e -> (e.getValue().contains(RExecutionPlanStepDataType.COLUMN_BUILT)) ? 1 : 0).sum();
        numberOfFollowUpSteps.put(outCol, provideDataForStepsColBuiltCount);

        for (RColOrValue fnParam : step.getDetailsFunction().getFunctionArguments()) {
          if (fnParam.isSetColumn()) {
            String inputCol = fnParam.getColumn().getColName();
            sourceColumns.get(outCol).add(inputCol);
          }
        }
        break;
      default:
      }
    }

    // Work on those steps whose output col is available already. They basically do not need to provide data to anyone
    // any more, as the cols are available already.
    // Note that this will /never/ happen for RepeatedProjectSteps, as their output column has '[*]' appended - that
    // column will never exist. This is because that step will not only create one, but multiple columns (a repeated
    // field). These steps will therefore /always/ run, even when all of their output cols would be in the cache,
    // but only when these results are needed only for a column which in turn is already cached. Running the
    // RepeatedProjectStep though is not as bad, as that step itself checks what columns it needs to create and which
    // ones are available.
    for (String colName : columnCreatingSteps.keySet())
      if (defaultEnv.getColumnShard(colName) != null) {
        logger.trace("Column {} is available already (cache). Will remove the corresponding step from the plan.",
            colName);
        numberOfFollowUpSteps.put(colName, 0);
      }

    Set<RExecutionPlanStep> stepsToDelete = new HashSet<>();

    // now keep searching the steps which have no output any more, marking the steps with no output for removal.
    Set<String> columnsWorkedOn = new HashSet<>();
    boolean changedSomething = true;
    while (changedSomething) {
      changedSomething = false;
      Set<String> columnsWithNoOutput = numberOfFollowUpSteps.entrySet().stream()
          .filter(entry -> entry.getValue().intValue() <= 0).map(entry -> entry.getKey()).collect(Collectors.toSet());
      columnsWithNoOutput = Sets.difference(columnsWithNoOutput, columnsWorkedOn);

      for (String colWithNoOutput : columnsWithNoOutput) {
        stepsToDelete.add(columnCreatingSteps.get(colWithNoOutput));
        for (String sourceCol : sourceColumns.get(colWithNoOutput)) {
          // only work on cols we know of - i.e. not on cols of the TableShard, but only on function-created cols.
          if (sourceColumns.containsKey(sourceCol)) {
            numberOfFollowUpSteps.compute(sourceCol, (k, count) -> count - 1);
            changedSomething = true;
          }
        }
      }

      columnsWorkedOn.addAll(columnsWithNoOutput);
    }

    if (!stepsToDelete.isEmpty()) {
      // remove the steps
      plan.getSteps().removeAll(stepsToDelete);

      logger.trace("Removing following steps from plan because their result is not needed: {}", stepsToDelete);

      // If any remaining step provided input to the removed steps, we need to remove that input provider.
      Set<Integer> stepIdsRemoved = stepsToDelete.stream().map(step -> step.getStepId()).collect(Collectors.toSet());
      for (RExecutionPlanStep step : plan.getSteps())
        if (step.isSetProvideDataForSteps() && !step.getProvideDataForSteps().isEmpty())
          step.getProvideDataForSteps().keySet().removeAll(stepIdsRemoved);
    }
  }
}
