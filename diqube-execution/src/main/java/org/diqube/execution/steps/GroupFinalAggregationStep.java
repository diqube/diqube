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
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.types.dbl.DoubleColumnShard;
import org.diqube.data.types.lng.LongColumnShard;
import org.diqube.data.types.str.StringColumnShard;
import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.consumers.AbstractThreadedGroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupFinalAggregationConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.VersionedExecutionEnvironment;
import org.diqube.function.AggregationFunction;
import org.diqube.function.FunctionException;
import org.diqube.function.FunctionFactory;
import org.diqube.function.IntermediaryResult;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.SparseColumnShardBuilder;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Receives {@link IntermediaryResult}s provided by {@link GroupIntermediaryAggregationStep}s and combines them to
 * actual values. The resulting column that is built (of which the outputs are informed by a {@link ColumnBuiltConsumer}
 * ) will contain just enough rows to contain all group results (groupId = rowId, finding the max of it). If a
 * {@link ColumnVersionBuiltConsumer} is wired, intermediary columns will be built.
 * 
 * <p>
 * Input: 1 {@link GroupIntermediaryAggregationConsumer} <br>
 * Output: {@link GroupFinalAggregationConsumer}, {@link ColumnBuiltConsumer}, {@link ColumnVersionBuiltConsumer}
 *
 * @author Bastian Gloeckle
 */
public class GroupFinalAggregationStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(GroupFinalAggregationStep.class);

  private AtomicBoolean sourceIsDone = new AtomicBoolean(false);

  private ConcurrentLinkedDeque<Triple<Long, IntermediaryResult, IntermediaryResult>> groupIntermediaryUpdates =
      new ConcurrentLinkedDeque<>();

  private AbstractThreadedGroupIntermediaryAggregationConsumer groupIntermediaryConsumer =
      new AbstractThreadedGroupIntermediaryAggregationConsumer(this) {

        @Override
        protected void allSourcesAreDone() {
          GroupFinalAggregationStep.this.sourceIsDone.set(true);
        }

        @Override
        protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
            IntermediaryResult oldIntermediaryResult, IntermediaryResult newIntermediaryResult) {
          if (newIntermediaryResult.getOutputColName().equals(outputColName))
            groupIntermediaryUpdates.add(new Triple<>(groupId, oldIntermediaryResult, newIntermediaryResult));
        }
      };

  private ExecutionEnvironment defaultEnv;
  private FunctionFactory functionFactory;
  private String functionNameLowerCase;
  private String outputColName;
  private Map<Long, AggregationFunction<Object, Object>> aggregationFunctions = new HashMap<>();

  private ColumnShardBuilderFactory columnShardBuilderFactory;

  private ColumnVersionManager columnVersionManager;

  private List<Object> constantFunctionParameters;

  private Set<Long> groupIdsChangedSinceLastOutputVersionBuilt = new HashSet<>();

  public GroupFinalAggregationStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      FunctionFactory functionFactory, ColumnShardBuilderFactory columnShardBuilderFactory,
      String functionNameLowerCase, String outputColName, ColumnVersionManager columnVersionManager,
      List<Object> constantFunctionParameters) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.functionFactory = functionFactory;
    this.columnShardBuilderFactory = columnShardBuilderFactory;
    this.functionNameLowerCase = functionNameLowerCase;
    this.outputColName = outputColName;
    this.columnVersionManager = columnVersionManager;
    this.constantFunctionParameters = constantFunctionParameters;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof GroupFinalAggregationConsumer)
        && !(consumer instanceof ColumnBuiltConsumer) && !(consumer instanceof ColumnVersionBuiltConsumer))
      throw new IllegalArgumentException(
          "Only GroupFinalAggregationConsumer, ColumnBuiltConsumer " + "and ColumnVersionBuiltConsumer supported.");
  }

  @Override
  protected void execute() {

    @SuppressWarnings("unchecked")
    Triple<Long, IntermediaryResult, IntermediaryResult>[] activeUpdates = new Triple[groupIntermediaryUpdates.size()];
    for (int i = 0; i < activeUpdates.length; i++)
      activeUpdates[i] = groupIntermediaryUpdates.poll();

    if (activeUpdates.length > 0) {
      Set<Long> groupIdsChanged = new HashSet<>();
      for (Triple<Long, IntermediaryResult, IntermediaryResult> update : activeUpdates) {
        Long groupId = update.getLeft();
        groupIdsChanged.add(groupId);
        IntermediaryResult oldIntermediary = update.getMiddle();
        IntermediaryResult newIntermediary = update.getRight();

        logger.trace("Processing update of group {} on col {}: new {}, old {}", groupId, outputColName, newIntermediary,
            oldIntermediary);

        if (!aggregationFunctions.containsKey(groupId)) {
          AggregationFunction<Object, Object> fn =
              functionFactory.createAggregationFunction(functionNameLowerCase, newIntermediary.getInputColumnType());

          if (fn == null)
            throw new ExecutablePlanExecutionException("Cannot find function '" + functionNameLowerCase
                + "' with input data type " + newIntermediary.getInputColumnType());

          for (int i = 0; i < constantFunctionParameters.size(); i++)
            fn.provideConstantParameter(i, constantFunctionParameters.get(i));

          fn.addIntermediary(newIntermediary.createValueIterator());
          aggregationFunctions.put(groupId, fn);
        } else {
          if (oldIntermediary != null)
            aggregationFunctions.get(groupId).removeIntermediary(oldIntermediary.createValueIterator());

          aggregationFunctions.get(groupId).addIntermediary(newIntermediary.createValueIterator());
        }
      }

      for (Long groupId : groupIdsChanged) {
        Object result = aggregationFunctions.get(groupId).calculate();

        logger.trace("New value for group {} on col {}: {}", groupId, outputColName, result);

        forEachOutputConsumerOfType(GroupFinalAggregationConsumer.class,
            c -> c.consumeAggregationResult(groupId, outputColName, result));
      }

      groupIdsChangedSinceLastOutputVersionBuilt.addAll(groupIdsChanged);
    }

    boolean isLastRun = sourceIsDone.get() && groupIntermediaryUpdates.isEmpty(); // Note: get "last run" value before
                                                                                  // building the col!

    ColumnShard newCol = null;
    // inform ColumnVersionBuiltConsumers (and build new version of column) if there are "enough" updates - do not do
    // this too often as it is pretty time consuming.
    if (activeUpdates.length > 0 && existsOutputConsumerOfType(ColumnVersionBuiltConsumer.class)) {

      // TODO #2 (stats): base this on stats.
      if ((groupIdsChangedSinceLastOutputVersionBuilt.size() >= 0.05 * aggregationFunctions.size()) || //
      /* */ // execute definitely if this is the last run: We need to send the changed group ids!
      /* */ isLastRun) {

        logger.trace("Creating new column version of {}, changed group IDs {}", outputColName,
            groupIdsChangedSinceLastOutputVersionBuilt);

        newCol = createNewColumn();
        VersionedExecutionEnvironment newEnv = columnVersionManager.createNewVersion(newCol);
        Set<Long> finalGroupIdsChanged = groupIdsChangedSinceLastOutputVersionBuilt;

        forEachOutputConsumerOfType(ColumnVersionBuiltConsumer.class,
            c -> c.columnVersionBuilt(newEnv, outputColName, finalGroupIdsChanged));
        groupIdsChangedSinceLastOutputVersionBuilt = new HashSet<>();
      }
    }

    // if done, inform other consumers.
    if (isLastRun) {
      if (!aggregationFunctions.isEmpty()) { // check if there is any result at all, if not, just report "done" (below).
        logger.trace("Creating final grouped column {}", outputColName);
        if (newCol == null)
          newCol = createNewColumn();

        switch (newCol.getColumnType()) {
        case STRING:
          defaultEnv.storeTemporaryStringColumnShard((StringColumnShard) newCol);
          break;
        case LONG:
          defaultEnv.storeTemporaryLongColumnShard((LongColumnShard) newCol);
          break;
        case DOUBLE:
          defaultEnv.storeTemporaryDoubleColumnShard((DoubleColumnShard) newCol);
          break;
        }

        forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(outputColName));
      }

      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  private ColumnShard createNewColumn() throws FunctionException {
    SparseColumnShardBuilder<Object> columnBuildManager =
        columnShardBuilderFactory.createSparseColumnShardBuilder(outputColName);

    Map<Long, Object> rowIdToValue = new HashMap<>();
    long maxRowId = -1;
    for (Long rowId : aggregationFunctions.keySet()) {
      rowIdToValue.put(rowId, aggregationFunctions.get(rowId).calculate());
      if (rowId > maxRowId)
        maxRowId = rowId;
    }

    logger.trace("Values of new col (limit): {}", Iterables.limit(rowIdToValue.entrySet(), 100));
    columnBuildManager.withNumberOfRows(maxRowId + 1).withValues(rowIdToValue);

    ColumnShard columnShard = columnBuildManager.build();
    return columnShard;
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { groupIntermediaryConsumer }));
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "funcName=" + functionNameLowerCase + ", outputCol=" + outputColName;
  }

}
