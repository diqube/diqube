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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.function.AggregationFunction;
import org.diqube.function.AggregationFunction.ValueProvider;
import org.diqube.function.FunctionFactory;
import org.diqube.function.IntermediaryResult;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;

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

  private static final int BATCH_SIZE = 100;

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
  private RepeatedColumnNameGenerator repeatedColName;
  private String inputColumnNamePattern;
  private Function<ColumnType, ColumnShardBuilderManager> columnShardBuilderManagerSupplier;

  public ColumnAggregationStep(int stepId, ExecutionEnvironment defaultEnv, RepeatedColumnNameGenerator repeatedColName,
      ColumnShardBuilderFactory columnShardBuilderFactory, FunctionFactory functionFactory,
      String functionNameLowerCase, String outputColName, String inputColumnNamePattern) {
    super(stepId);
    this.defaultEnv = defaultEnv;
    this.repeatedColName = repeatedColName;
    this.functionFactory = functionFactory;
    this.functionNameLowerCase = functionNameLowerCase;
    this.outputColName = outputColName;
    this.inputColumnNamePattern = inputColumnNamePattern;

    columnShardBuilderManagerSupplier = (outputColType) -> {
      LoaderColumnInfo columnInfo = new LoaderColumnInfo(outputColType);
      return columnShardBuilderFactory.createColumnShardBuilderManager(columnInfo, defaultEnv.getFirstRowIdInShard());
    };
  }

  @Override
  protected void execute() {
    // validate if all "length" columns are available and all [index] columns, too - we do this by looking for all
    // columns with all indices that are contained in the length columns (= the maximum).
    Set<String> allColNames = findColNamesForColNamePattern(inputColumnNamePattern, 0, "",
        lenCol -> lenCol.getColumnShardDictionary().decompressValue(lenCol.getColumnShardDictionary().getMaxId()));

    if (allColNames == null) {
      // not all length cols were available.
      if (allColumnsAreBuilt.get()) {
        // retry because to not face race conditions
        allColNames = findColNamesForColNamePattern(inputColumnNamePattern, 0, "",
            lenCol -> lenCol.getColumnShardDictionary().decompressValue(lenCol.getColumnShardDictionary().getMaxId()));
        if (allColNames == null)
          // still not all length cols available although all input columns should be built already, there is an error!
          throw new ExecutablePlanExecutionException("When trying to aggregate column values, not all repeated "
              + "columns were available. It was expected to have repeated columns where the input column pattern "
              + "specifies '" + repeatedColName.allEntriesIdentifyingSubstr()
              + "'. Perhaps not all of these columns are repeated columns?");
      } else
        return;
    }
    if (allColNames.isEmpty())
      throw new ExecutablePlanExecutionException(
          "Input col name pattern did not contain '" + repeatedColName.allEntriesIdentifyingSubstr() + "'.");

    // all length columns are available, check all the [index] columns now.
    boolean notAllColsAvailable =
        allColNames.stream().anyMatch(requiredCol -> defaultEnv.getColumnShard(requiredCol) == null);
    if (notAllColsAvailable)
      return;

    // Ok, all columns that we need seem to be available.

    ColumnType inputColType = defaultEnv.getColumnShard(Iterables.getFirst(allColNames, null)).getColumnType();
    AggregationFunction<Object, IntermediaryResult<Object, Object, Object>, Object> tmpFunction =
        functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

    ColumnShardBuilderManager colShardBuilderManager =
        columnShardBuilderManagerSupplier.apply(tmpFunction.getOutputType());

    long lastRowIdInShard = defaultEnv.getLastRowIdInShard();
    final Set<String> finalAllColNames = allColNames;

    // work on all rowIds with a specific batch size
    LongStream.rangeClosed(defaultEnv.getFirstRowIdInShard(), lastRowIdInShard). //
        parallel().filter(l -> l % BATCH_SIZE == 0).forEach(new LongConsumer() {
          @Override
          public void accept(long firstRowId) {
            // to not have to decompress a lot of single values later, lets decompress all values of the current batch
            // for all columns.
            // This might resolve some columns that we later do not need for some columns, but anyway, in general it
            // should be faster.
            Class<?> valueClass = null;
            Map<String, Object[]> valuesByCol = new HashMap<>();
            for (String inputColName : finalAllColNames) {
              ColumnShard colShard = defaultEnv.getColumnShard(inputColName);
              Long[] colValueIds = colShard.resolveColumnValueIdsForRowsFlat(
                  LongStream.range(firstRowId, Math.min(firstRowId + BATCH_SIZE, lastRowIdInShard + 1))
                      .mapToObj(Long::valueOf).toArray(l -> new Long[l]));
              Object[] values = colShard.getColumnShardDictionary().decompressValues(colValueIds);
              if (valueClass == null)
                valueClass = values[0].getClass();
              valuesByCol.put(inputColName, values);
            }

            final Class<?> finalValueClass = valueClass;

            Object[] resValueArray = null;
            for (long rowId = firstRowId; rowId < firstRowId + BATCH_SIZE && rowId <= lastRowIdInShard; rowId++) {

              // Ok, lets work on this single row. Let's first find all the column names that are important for this row
              // - based on the value of the "length" columns at this row. The other rows (with indices >= length) will
              // contain some sort of default values, which we do not want to include in the aggregation!
              final long finalRowId = rowId;
              Set<String> colNamesForCurRow = findColNamesForColNamePattern(inputColumnNamePattern, 0, "", lenCol -> {
                long colValueId = lenCol.resolveColumnValueIdForRow(finalRowId);
                return lenCol.getColumnShardDictionary().decompressValue(colValueId);
              });

              AggregationFunction<Object, IntermediaryResult<Object, Object, Object>, Object> aggFunction =
                  functionFactory.createAggregationFunction(functionNameLowerCase, inputColType);

              // add the values to the aggregation, resolve them using the pre-computed arrays from above.
              aggFunction.addValues(new ValueProvider<Object>() {
                @Override
                public Object[] getValues() {
                  Object[] res = colNamesForCurRow.stream()
                      .map(colName -> valuesByCol.get(colName)[(int) (finalRowId - firstRowId)])
                      .toArray(l -> (Object[]) Array.newInstance(finalValueClass, colNamesForCurRow.size()));
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

            colShardBuilderManager.addValues(outputColName, resValueArray, firstRowId);
          }
        });

    ColumnShard outputCol = colShardBuilderManager.build(outputColName);

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
    if (!(consumer instanceof ColumnBuiltConsumer))
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

  /**
   * Replaces all the [*] strings in the pattern with actual column indices.
   * 
   * @param pattern
   *          The column name pattern
   * @param startIdx
   *          The index in the pattern string to start at. Typically on the first call this is 0.
   * @param parentColName
   *          while this method is being executed, it will recursively call itself. Then this string contains the
   *          current column name of everything in the pattern before startIdx - with the actual indices already
   *          inserted. Provide "" when calling this method from the outside.
   * @param lengthProvider
   *          Function to find out how many objects should be assumed to be contained for a column. Parameter to this
   *          method is the "length" column (= the column of a repeated column that contains the number of elements in
   *          that repeated column for each row). Callers can e.g. resolve the number of elements for a specific rowId
   *          in "lengthProvider" or they could simply take the "max" value of the whole column to make this method
   *          return a list of all possible column names that could be valid for all rows in the table.
   * @return Set of string, the final column names based on the lengths provided by the lengthProvider. The returned set
   *         will be empty, if there is no [*] substring in the pattern. It will be <code>null</code> if not for all
   *         columns "length" columns are available where the input pattern has a [*].
   */
  private Set<String> findColNamesForColNamePattern(String pattern, int startIdx, String parentColName,
      Function<LongColumnShard, Long> lengthProvider) {
    int allEntriesIdentifierLength = repeatedColName.allEntriesIdentifyingSubstr().length();

    Set<String> res = new HashSet<>();
    int idx = pattern.indexOf(repeatedColName.allEntriesIdentifyingSubstr(), startIdx);
    if (idx == -1)
      return res;
    String baseName;
    if (startIdx == 0)
      baseName = pattern.substring(startIdx, idx);
    else
      baseName = pattern.substring(startIdx + 1 /* skip previous . */, idx);

    if (!parentColName.equals(""))
      parentColName += ".";

    String lenColName = repeatedColName.repeatedLength(parentColName + baseName);
    LongColumnShard lenCol = defaultEnv.getLongColumnShard(lenColName);
    if (lenCol == null)
      return null;

    boolean noMorePatterns = false;
    long len = lengthProvider.apply(lenCol);
    for (long i = 0; i < len; i++) {
      String curColName = parentColName + repeatedColName.repeatedAtIndex(baseName, i);
      if (!noMorePatterns) {
        Set<String> nextRes = findColNamesForColNamePattern(pattern,
            startIdx + baseName.length() + allEntriesIdentifierLength, curColName, lengthProvider);
        if (nextRes == null)
          // no length col available
          return null;
        if (nextRes.isEmpty()) {
          // no more patterns left, process below.
          noMorePatterns = true;
        } else {
          // recursive call identified some solutions ad added the full names to its result.
          res.addAll(nextRes);
        }
      }
      if (noMorePatterns) {
        StringBuilder sb = new StringBuilder();
        sb.append(curColName);
        if (idx + allEntriesIdentifierLength < pattern.length())
          sb.append(pattern.substring(idx + allEntriesIdentifierLength));
        res.add(sb.toString());
      }
    }
    return res;
  }

}
