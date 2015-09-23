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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.diqube.data.Table;
import org.diqube.data.TableShard;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.execution.consumers.GroupFinalAggregationConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.OrderedRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironmentFactory;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.steps.GroupIntermediaryAggregationStep;
import org.diqube.execution.steps.ResolveValuesStep;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDataType;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds an {@link ExecutablePlan} out of a {@link RExecutionPlan}.
 * 
 * <p>
 * This builder is not that intelligent: It simply instantiates the correct {@link ExecutablePlanStep}s as defined by
 * the {@link RExecutionPlanStep} and uses the wiring defined there.
 *
 * @author Bastian Gloeckle
 */
public class ExecutablePlanFromRemoteBuilder {

  private static final Logger logger = LoggerFactory.getLogger(ExecutablePlanFromRemoteBuilder.class);

  private static final Map<RExecutionPlanStepDataType, Class<? extends GenericConsumer>> stepDataTypeToConsumerClass =
      new HashMap<>();

  private RExecutionPlan plan;
  private TableRegistry tableRegistry;
  private ExecutionEnvironmentFactory executionEnvironmentFactory;
  private ExecutablePlanStepFromRemoteFactory executablePlanStepFactory;
  private ColumnValueConsumer columnValueConsumer;
  private GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer;

  private ExecutablePlanFactory executablePlanFactory;

  /* package */ ExecutablePlanFromRemoteBuilder(TableRegistry tableRegistry,
      ExecutionEnvironmentFactory executionEnvironmentFactory,
      ExecutablePlanStepFromRemoteFactory executablePlanStepFactory, ExecutablePlanFactory executablePlanFactory) {
    this.tableRegistry = tableRegistry;
    this.executionEnvironmentFactory = executionEnvironmentFactory;
    this.executablePlanStepFactory = executablePlanStepFactory;
    this.executablePlanFactory = executablePlanFactory;
  }

  /**
   * Use the given {@link RExecutionPlanStep} to build from.
   */
  public ExecutablePlanFromRemoteBuilder withRemoteExecutionPlan(RExecutionPlan plan) {
    this.plan = plan;
    return this;
  }

  /**
   * Send all data that was finally loaded to the given {@link ColumnValueConsumer}.
   */
  public ExecutablePlanFromRemoteBuilder withFinalColumnValueConsumer(ColumnValueConsumer columnValueConsumer) {
    this.columnValueConsumer = columnValueConsumer;
    return this;
  }

  /**
   * Send the results of all {@link GroupIntermediaryAggregationStep} to the given
   * {@link GroupIntermediaryAggregationConsumer}.
   */
  public ExecutablePlanFromRemoteBuilder withFinalGroupIntermediateAggregationConsumer(
      GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer) {
    this.groupIntermediaryAggregationConsumer = groupIntermediaryAggregationConsumer;
    return this;
  }

  /**
   * Build the {@link ExecutablePlan}s, for each {@link TableShard} that is available on this node one.
   * 
   * <p>
   * This method must be executed with correct {@link QueryUuidThreadState} set, as
   * {@link RemoteExecutionPlanOptimizer} needs correct thread state!
   */
  public List<ExecutablePlan> build() throws ExecutablePlanBuildException {
    Table table = tableRegistry.getTable(plan.getTable());
    if (table == null) {
      throw new ExecutablePlanBuildException("Table '" + plan.getTable() + "' does not exist.");
    }

    List<ExecutablePlan> res = new ArrayList<>(table.getShards().size());

    for (TableShard tableShard : table.getShards()) {
      ExecutionEnvironment defaultEnv = executionEnvironmentFactory.createQueryRemoteExecutionEnvironment(tableShard);
      Map<Integer, ExecutablePlanStep> steps = new HashMap<>();
      Map<Integer, RExecutionPlanStep> remoteSteps = new HashMap<>();

      // note that the following optimization might already put some columns in the Env (from the ColumnShardCache).
      RExecutionPlan optimizedRemotePlan = new RemoteExecutionPlanOptimizer().optimize(defaultEnv, plan);

      for (RExecutionPlanStep remoteStep : optimizedRemotePlan.getSteps()) {
        ExecutablePlanStep newStep = executablePlanStepFactory.createExecutableStep(defaultEnv, remoteStep);
        steps.put(remoteStep.getStepId(), newStep);
        remoteSteps.put(remoteStep.getStepId(), remoteStep);
      }

      // Wire the data flow.
      for (Entry<Integer, ExecutablePlanStep> stepEntry : steps.entrySet()) {
        ExecutablePlanStep sourceStep = stepEntry.getValue();
        RExecutionPlanStep remoteStep = remoteSteps.get(sourceStep.getStepId());

        // Use the data flow specifications from the RExecutionPlan.
        if (remoteStep.getProvideDataForStepsSize() > 0) {
          for (Entry<Integer, List<RExecutionPlanStepDataType>> targetEntry : remoteStep.getProvideDataForSteps()
              .entrySet()) {
            int targetIdx = targetEntry.getKey();

            ExecutablePlanStep targetStep = steps.get(targetIdx);
            if (targetStep == null)
              throw new ExecutablePlanBuildException("Could not find data flow target.");

            for (RExecutionPlanStepDataType type : targetEntry.getValue()) {
              logger.trace("Wiring {} from {} to {}",
                  new Object[] { stepDataTypeToConsumerClass.get(type), sourceStep, targetStep });
              targetStep.wireOneInputConsumerToOutputOf(stepDataTypeToConsumerClass.get(type), sourceStep);
            }
          }
        }

        // add the manually specified ColumnValueConsumer to the ResolveValueStep (which should be exactly one and which
        // should not have an output consumer set yet).
        if (sourceStep instanceof ResolveValuesStep)
          sourceStep.addOutputConsumer(columnValueConsumer);

        if (sourceStep instanceof GroupIntermediaryAggregationStep && groupIntermediaryAggregationConsumer != null)
          sourceStep.addOutputConsumer(groupIntermediaryAggregationConsumer);
      }

      ExecutablePlanInfo info = createExecutablePlanInfo(optimizedRemotePlan);
      ExecutablePlan executablePlan =
          executablePlanFactory.createExecutablePlan(defaultEnv, new ArrayList<>(steps.values()), info,
              null /* no col version manager on remote as there are no colversions used here */);
      res.add(executablePlan);
    }

    return res;
  }

  private ExecutablePlanInfo createExecutablePlanInfo(RExecutionPlan plan) {
    List<String> selectedCols = new ArrayList<>();

    selectedCols
        .addAll(plan.getSteps().stream().filter(s -> s.getType().equals(RExecutionPlanStepType.RESOLVE_COLUMN_DICT_IDS))
            .map(s -> s.getDetailsResolve().getColumn().getColName()).collect(Collectors.toList()));

    boolean isOrdered = plan.getSteps().stream().anyMatch(s -> s.getType().equals(RExecutionPlanStepType.ORDER));
    boolean isGrouped = plan.getSteps().stream().anyMatch(s -> s.getType().equals(RExecutionPlanStepType.GROUP));

    return executablePlanFactory.createExecutablePlanInfo(selectedCols, isOrdered, isGrouped,
        false /* there cannot be a HAVING, because were on a query remote */);
  }

  static {
    // keep in sync with RemoteWireManager
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.COLUMN_BUILT, ColumnBuiltConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.COLUMN_DICT_ID, ColumnDictIdConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.COLUMN_VALUE, ColumnValueConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.GROUP, GroupConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.GROUP_DELTA, GroupDeltaConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.GROUP_FINAL_AGG, GroupFinalAggregationConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.GROUP_INTERMEDIARY_AGG,
        GroupIntermediaryAggregationConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.ORDERED_ROW_ID, OrderedRowIdConsumer.class);
    stepDataTypeToConsumerClass.put(RExecutionPlanStepDataType.ROW_ID, RowIdConsumer.class);
  }
}
