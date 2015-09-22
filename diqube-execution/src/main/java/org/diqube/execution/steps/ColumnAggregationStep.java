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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.execution.util.ColumnPatternUtil;
import org.diqube.execution.util.ColumnPatternUtil.ColumnPatternContainer;
import org.diqube.execution.util.ColumnPatternUtil.LengthColumnMissingException;
import org.diqube.function.AggregationFunction;
import org.diqube.function.AggregationFunction.ValueProvider;
import org.diqube.function.FunctionFactory;
import org.diqube.function.IntermediaryResult;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilder;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Step that aggregates the values of multiple columns in a single row to a single value.
 * 
 * In contrast to {@link GroupIntermediaryAggregationStep} and {@link GroupFinalAggregationStep}, this does <b>not</b>
 * aggregate values of multiple rows. Therefore this can be fully executed on query remotes.
 * 
 * <p>
 * Input: 1 optional {@link ColumnBuiltConsumer}, <br>
 * Output: {@link ColumnBuiltConsumer}
 *
 * @author Bastian Gloeckle
 */
public class ColumnAggregationStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(ColumnAggregationStep.class);

  private static final int BATCH_SIZE = ColumnShardBuilder.PROPOSAL_ROWS / 2; // approx work on half ColumnPages.

  private AtomicBoolean allColumnsAreBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer columnBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      allColumnsAreBuilt.set(true);
    }

    @Override
    protected void doColumnBuilt(String colName) {
      // noop, we try new each time.
    }
  };
  private FunctionFactory functionFactory;
  private String functionNameLowerCase;
  private String outputColName;
  private ExecutionEnvironment defaultEnv;
  private ColumnShardBuilderFactory columnShardBuilderFactory;
  private String inputColumnNamePattern;
  private Function<ColumnType, ColumnShardBuilderManager> columnShardBuilderManagerSupplier;
  private ColumnPatternUtil columnPatternUtil;
  private List<Object> constantFunctionParameters;

  public ColumnAggregationStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      ColumnPatternUtil columnPatternUtil, ColumnShardBuilderFactory columnShardBuilderFactory,
      FunctionFactory functionFactory, String functionNameLowerCase, String outputColName,
      String inputColumnNamePattern, List<Object> constantFunctionParameters) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.columnPatternUtil = columnPatternUtil;
    this.columnShardBuilderFactory = columnShardBuilderFactory;
    this.functionFactory = functionFactory;
    this.functionNameLowerCase = functionNameLowerCase;
    this.outputColName = outputColName;
    this.inputColumnNamePattern = inputColumnNamePattern;
    this.constantFunctionParameters = constantFunctionParameters;
  }

  @Override
  public void initialize() {
    columnShardBuilderManagerSupplier = (outputColType) -> {
      LoaderColumnInfo columnInfo = new LoaderColumnInfo(outputColType);
      return columnShardBuilderFactory.createColumnShardBuilderManager(columnInfo, defaultEnv.getFirstRowIdInShard());
    };
  };

  @Override
  protected void execute() {
    boolean lastRun = allColumnsAreBuilt.get();

    // validate if all "length" columns are available and all [index] columns, too - we do this by looking for all
    // columns with all indices that are contained in the length columns (= the maximum).
    Set<String> allColNames;
    ColumnPatternContainer columnPatternContainer;
    try {
      columnPatternContainer = columnPatternUtil.findColNamesForColNamePattern(defaultEnv, inputColumnNamePattern);
      allColNames = columnPatternContainer.getMaximumColumnPatternsSinglePattern();
    } catch (LengthColumnMissingException e) {
      if (lastRun)
        throw new ExecutablePlanExecutionException("When trying to aggregate column values, not all repeated "
            + "columns were available. It was expected to have repeated columns where the input column pattern "
            + "specifies '[*]'. Perhaps not all of these columns are repeated columns?");
      return;
    }

    if (allColNames.isEmpty())
      throw new ExecutablePlanExecutionException("Input col name pattern did not contain '[*]'.");

    // all length columns are available, check all the [index] columns now.
    boolean notAllColsAvailable =
        allColNames.stream().anyMatch(requiredCol -> defaultEnv.getColumnShard(requiredCol) == null);
    if (notAllColsAvailable) {
      logger.trace("Columns {} missing. Not proceeding.", allColNames.stream()
          .filter(reqiredCol -> defaultEnv.getColumnShard(reqiredCol) == null).collect(Collectors.toList()));

      if (lastRun)
        throw new ExecutablePlanExecutionException("When trying to aggregate column values, not all repeated "
            + "columns were available. It was expected to have repeated columns where the input column pattern "
            + "specifies '[*]'. Perhaps not all of these columns are repeated columns?");

      return;
    }

    // Ok, all columns that we need seem to be available.

    logger.trace("Starting to column aggregate with output col {}", outputColName);

    ColumnType inputColType = defaultEnv.getColumnShard(Iterables.getFirst(allColNames, null)).getColumnType();
    AggregationFunction<Object, IntermediaryResult<Object, Object, Object>, Object> tmpFunction =
        functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

    if (tmpFunction == null)
      throw new ExecutablePlanExecutionException(
          "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

    ColumnShardBuilderManager colShardBuilderManager =
        columnShardBuilderManagerSupplier.apply(tmpFunction.getOutputType());

    long lastRowIdInShard = defaultEnv.getLastRowIdInShard();
    final Map<String, Integer> finalAllColNames = new HashMap<>();
    int tmp = 0;
    for (String colName : allColNames)
      finalAllColNames.put(colName, tmp++);

    final ColumnPatternContainer finalColumnPatternContainer = columnPatternContainer;

    QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();
    // work on all rowIds with a specific batch size
    LongStream.rangeClosed(defaultEnv.getFirstRowIdInShard(), lastRowIdInShard). //
        parallel().filter(l -> (l - defaultEnv.getFirstRowIdInShard()) % BATCH_SIZE == 0).forEach(new LongConsumer() {
          @Override
          public void accept(long firstRowId) {
            QueryUuid.setCurrentThreadState(uuidState);
            try {
              // to not have to decompress a lot of single values later, lets decompress all values of the current batch
              // for all columns.
              // This might resolve some columns that we later do not need for some columns, but anyway, in general it
              // should be faster.
              logger.trace("Resolving colValue IDs for batch {}", firstRowId);
              Class<?> valueClass = null;
              Object[][] valuesByCol = new Object[finalAllColNames.size()][];
              Long[] rowIds = LongStream.range(firstRowId, Math.min(firstRowId + BATCH_SIZE, lastRowIdInShard + 1))
                  .mapToObj(Long::valueOf).toArray(l -> new Long[l]);
              for (Entry<String, Integer> inputColNameEntry : finalAllColNames.entrySet()) {
                String inputColName = inputColNameEntry.getKey();

                QueryableColumnShard colShard = defaultEnv.getColumnShard(inputColName);
                Long[] colValueIds = colShard.resolveColumnValueIdsForRowsFlat(rowIds);
                Object[] values = colShard.getColumnShardDictionary().decompressValues(colValueIds);
                if (valueClass == null)
                  valueClass = values[0].getClass();
                valuesByCol[inputColNameEntry.getValue()] = values;
              }
              logger.trace("ColValue IDs for batch {} resolved.", firstRowId);

              final Class<?> finalValueClass = valueClass;

              logger.trace("Starting to apply aggregation function to all rows in batch {}", firstRowId);
              Object[] resValueArray = null;
              for (long rowId = firstRowId; rowId < firstRowId + BATCH_SIZE && rowId <= lastRowIdInShard; rowId++) {

                // Ok, lets work on this single row. Let's first find all the column names that are important for this
                // row - based on the value of the "length" columns at this row. The other rows (with indices >= length)
                // will contain some sort of default values, which we do not want to include in the aggregation!
                final long finalRowId = rowId;
                Set<String> colNamesForCurRow = finalColumnPatternContainer.getColumnPatternsSinglePattern(finalRowId);

                AggregationFunction<Object, IntermediaryResult<Object, Object, Object>, Object> aggFunction =
                    functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

                for (int i = 0; i < constantFunctionParameters.size(); i++)
                  aggFunction.provideConstantParameter(i, constantFunctionParameters.get(i));

                // add the values to the aggregation, resolve them using the pre-computed arrays from above.
                aggFunction.addValues(new ValueProvider<Object>() {
                  @Override
                  public Object[] getValues() {
                    Object[] res = (Object[]) Array.newInstance(finalValueClass, colNamesForCurRow.size());
                    int i = 0;
                    int colIndices[] = new int[colNamesForCurRow.size()];
                    for (String colName : colNamesForCurRow) {
                      colIndices[i++] = finalAllColNames.get(colName);
                    }
                    int rowIndex = (int) (finalRowId - firstRowId);
                    for (int j = 0; j < colIndices.length; j++) {
                      res[j] = valuesByCol[colIndices[j]][rowIndex];
                    }
                    return res;
                  }

                  @Override
                  public long size() {
                    return colNamesForCurRow.size();
                  }
                });

                Object resValue = aggFunction.calculate();

                // check if we still need to create the result array
                if (resValueArray == null) {
                  if (firstRowId + BATCH_SIZE <= lastRowIdInShard)
                    resValueArray = (Object[]) Array.newInstance(resValue.getClass(), BATCH_SIZE);
                  else
                    resValueArray =
                        (Object[]) Array.newInstance(resValue.getClass(), (int) (lastRowIdInShard - firstRowId + 1));
                }

                resValueArray[(int) (rowId - firstRowId)] = resValue;
              }

              logger.trace("Aggregation function applied to all rows in batch {}", firstRowId);
              colShardBuilderManager.addValues(outputColName, resValueArray, firstRowId);
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        });

    QueryUuid.setCurrentThreadState(uuidState);

    logger.trace("Building output column {}", outputColName);
    ColumnShard outputCol = colShardBuilderManager.buildAndFree(outputColName);
    logger.trace("Column {} built.", outputColName);

    switch (outputCol.getColumnType()) {
    case STRING:
      defaultEnv.storeTemporaryStringColumnShard((StringColumnShard) outputCol);
      break;
    case LONG:
      defaultEnv.storeTemporaryLongColumnShard((LongColumnShard) outputCol);
      break;
    case DOUBLE:
      defaultEnv.storeTemporaryDoubleColumnShard((DoubleColumnShard) outputCol);
      break;
    }

    forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(outputColName));
    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
    doneProcessing();
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof ColumnBuiltConsumer))
      throw new IllegalArgumentException("Only ColumnBuiltConsumer supported.");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(columnBuiltConsumer);
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // noop. if input is wired it's fine, if not, that's fine too.
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "outputColName=" + outputColName;
  }

}
