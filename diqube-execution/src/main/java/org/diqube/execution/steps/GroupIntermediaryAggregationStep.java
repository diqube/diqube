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
package org.diqube.execution.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.data.column.ColumnType;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupDeltaConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.function.AggregationFunction;
import org.diqube.function.AggregationFunction.ValueProvider;
import org.diqube.function.FunctionFactory;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates intermediary results of group aggregation function. This is typically executed on all cluster nodes,
 * whereas the intermediary results are provided to the query master which then combines the intermediary results from
 * all cluster nodes for one group (which is done by {@link GroupFinalAggregationStep}).
 * 
 * <p>
 * This step does not support a {@link ColumnVersionBuiltConsumer} as the step will be executed on remotes only and not
 * on the query master. While executing on remotes, no intermediary columns are supported.
 * 
 * <p>
 * Input: 1 {@link GroupDeltaConsumer}, multiple optional {@link ColumnBuiltConsumer}<br>
 * Output: {@link GroupIntermediaryAggregationConsumer}
 *
 * @author Bastian Gloeckle
 */
public class GroupIntermediaryAggregationStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(GroupIntermediaryAggregationStep.class);

  private AtomicBoolean groupDeltaSourceIsDone = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Map<Long, List<Long>>> newGroupChanges = new ConcurrentLinkedDeque<>();

  private AbstractThreadedGroupDeltaConsumer groupDeltaConsumer = new AbstractThreadedGroupDeltaConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      GroupIntermediaryAggregationStep.this.groupDeltaSourceIsDone.set(true);
    }

    @Override
    protected void doConsumeGroupDeltas(Map<Long, List<Long>> lastChangedGroups) {
      GroupIntermediaryAggregationStep.this.newGroupChanges.add(lastChangedGroups);
    }
  };

  private AtomicBoolean allColumnsBuilt = new AtomicBoolean(false);
  private AbstractThreadedColumnBuiltConsumer columnBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      GroupIntermediaryAggregationStep.this.allColumnsBuilt.set(true);

    }

    @Override
    protected void doColumnBuilt(String colName) {
    }
  };

  private ExecutionEnvironment env;
  private FunctionFactory functionFactory;
  private String functionNameLowerCase;
  private String outputColName;
  private Map<Long, AggregationFunction<Object, Object>> aggregationFunctions = new HashMap<>();
  /** can be null if no parameter is specified for the aggregation function (e.g. count()) */
  private String inputColumnName;

  private List<Object> constantFunctionParameters;

  public GroupIntermediaryAggregationStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment env,
      FunctionFactory functionFactory, String functionNameLowerCase, String outputColName, String inputColumnName,
      List<Object> constantFunctionParameters) {
    super(stepId, queryRegistry);
    this.env = env;
    this.functionFactory = functionFactory;
    this.functionNameLowerCase = functionNameLowerCase;
    this.outputColName = outputColName;
    this.inputColumnName = inputColumnName;
    this.constantFunctionParameters = constantFunctionParameters;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof GroupIntermediaryAggregationConsumer))
      throw new IllegalArgumentException("Only GroupIntermediaryAggregationConsumer supported.");
  }

  @Override
  protected void execute() {
    if (columnBuiltConsumer.getNumberOfTimesWired() > 0 && !allColumnsBuilt.get())
      // wait until input columns are built.
      return;

    List<Map<Long, List<Long>>> activeGroupDeltas = new ArrayList<>();
    Map<Long, List<Long>> grpDelta;
    while ((grpDelta = newGroupChanges.poll()) != null)
      activeGroupDeltas.add(grpDelta);

    if (activeGroupDeltas.size() > 0) {
      // merge group deltas
      Map<Long, List<Long>> groupDeltas = new HashMap<>();
      for (Map<Long, List<Long>> activeGrpDelta : activeGroupDeltas) {
        for (Entry<Long, List<Long>> activeEntry : activeGrpDelta.entrySet()) {
          if (!groupDeltas.containsKey(activeEntry.getKey()))
            groupDeltas.put(activeEntry.getKey(), new ArrayList<Long>());
          groupDeltas.get(activeEntry.getKey()).addAll(activeEntry.getValue());
        }
      }

      ColumnType inputColType;
      if (inputColumnName == null)
        inputColType = null;
      else
        inputColType = env.getColumnType(inputColumnName);
      AggregationFunction<Object, Object> tmpFn =
          functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

      if (tmpFn == null)
        throw new ExecutablePlanExecutionException(
            "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

      // map from groupId to array of colShardIds, may be null if not pre-resolved.
      Map<Long, Long[]> preResolvedColShardIds;

      if (tmpFn.needsActualValues()) {
        // we pre-resolve all values, as this should speed things up heavily if the input column is RunLength encoded.
        preResolvedColShardIds = new HashMap<>();

        Set<Long> allRowIds = new HashSet<>();
        for (Entry<Long, List<Long>> deltaEntry : groupDeltas.entrySet())
          allRowIds.addAll(deltaEntry.getValue());

        Map<Long, Long> rowIdToColShardId = env.getColumnShard(inputColumnName).resolveColumnValueIdsForRows(allRowIds);

        for (Entry<Long, List<Long>> deltaEntry : groupDeltas.entrySet()) {
          Long[] colShardIds = new Long[deltaEntry.getValue().size()];
          for (int i = 0; i < colShardIds.length; i++)
            colShardIds[i] = rowIdToColShardId.get(deltaEntry.getValue().get(i));
          preResolvedColShardIds.put(deltaEntry.getKey(), colShardIds);
        }
        logger.trace("Pre-resolved column shard IDs for {} groups.", groupDeltas.size());
      } else
        preResolvedColShardIds = null;

      for (Entry<Long, List<Long>> groupDeltaEntry : groupDeltas.entrySet()) {
        Long groupId = groupDeltaEntry.getKey();
        List<Long> newRowIds = groupDeltaEntry.getValue();
        if (!aggregationFunctions.containsKey(groupId)) {
          AggregationFunction<Object, Object> newFn =
              functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

          if (newFn == null)
            throw new ExecutablePlanExecutionException(
                "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

          for (int i = 0; i < constantFunctionParameters.size(); i++)
            newFn.provideConstantParameter(i, constantFunctionParameters.get(i));

          aggregationFunctions.put(groupId, newFn);
        }

        IntermediaryResult oldIntermediary = new IntermediaryResult(outputColName, inputColType);
        aggregationFunctions.get(groupId).populateIntermediary(oldIntermediary);

        // update AggregationFunction object with new values.
        aggregationFunctions.get(groupId).addValues(new ValueProvider<Object>() {
          @Override
          public Object[] getValues() {
            Long[] columnValueIds;
            if (preResolvedColShardIds != null && preResolvedColShardIds.containsKey(groupId))
              columnValueIds = preResolvedColShardIds.get(groupId);
            else
              columnValueIds = env.getColumnShard(inputColumnName).resolveColumnValueIdsForRowsFlat(newRowIds);

            return env.getColumnShard(inputColumnName).getColumnShardDictionary().decompressValues(columnValueIds);
          }

          @Override
          public long size() {
            return newRowIds.size();
          }
        });

        IntermediaryResult newIntermediary = new IntermediaryResult(outputColName, inputColType);
        aggregationFunctions.get(groupId).populateIntermediary(newIntermediary);

        logger.trace("New intermediary for group {} in col {}: new {}, old: {}", groupId, outputColName,
            newIntermediary, oldIntermediary);

        forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class,
            c -> c.consumeIntermediaryAggregationResult(groupId, outputColName, oldIntermediary, newIntermediary));
      }

    }

    if (groupDeltaSourceIsDone.get() && newGroupChanges.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { groupDeltaConsumer, columnBuiltConsumer }));
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "funcName=" + functionNameLowerCase + ", outputCol=" + outputColName;
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    if (groupDeltaConsumer.getNumberOfTimesWired() != 1)
      throw new ExecutablePlanBuildException("Group Delta input not wired.");
    // columnBuiltConsumer can be wired optionally.
  }
}
