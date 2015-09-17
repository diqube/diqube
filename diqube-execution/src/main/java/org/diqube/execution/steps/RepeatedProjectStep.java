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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.data.util.RepeatedColumnNameGenerator;
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
import org.diqube.function.FunctionFactory;
import org.diqube.function.ProjectionFunction;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilder;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.ColumnOrValue.Type;

import com.google.common.collect.Sets;

/**
 * This step executes {@link ProjectionFunction}s which rely on all elements of repeated fields (diql "[*]" syntax).
 * 
 * The result of this step will be another repeated column, which applies the function to each column name combination
 * that is generated by {@link ColumnPatternUtil#findColNamesForColNamePattern(ExecutionEnvironment, List, Function)} -
 * each function execution will then fill a new index in the resulting repeated column. The length of the repeated field
 * can, of course be different for each row.
 * 
 * This step is therefore a bit special: When processing an incoming diql string, special care has to be taken, that
 * such a repeated projection is used correctly.
 * 
 * <p>
 * This step is pretty expensive, although it tries to calculate the values of similar-looking rows together.
 * 
 * TODO #26: Reasonable creation of non-cached columns
 * 
 * <p>
 * Input: multiple optional {@link ColumnBuiltConsumer} <br>
 * Output: {@link ColumnBuiltConsumer} (the resulting "length" column will be published last to this consumer).
 *
 * @author Bastian Gloeckle
 */
public class RepeatedProjectStep extends AbstractThreadedExecutablePlanStep {
  private static final int BATCH_SIZE = ColumnShardBuilder.PROPOSAL_ROWS; // approx work on whole ColumnPages.

  private AtomicBoolean allInputColumnsBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      allInputColumnsBuilt.set(true);
    }

    @Override
    protected void doColumnBuilt(String colName) {
      // noop, we retry if all cols are available at each call to "execute".
    }
  };

  private String outputColNameBase;

  private ExecutionEnvironment defaultEnv;

  private FunctionFactory functionFactory;

  private String functionNameLowerCase;

  private ColumnOrValue[] functionParameters;

  private ColumnPatternUtil columnPatternUtil;

  private List<String> inputColPatterns;

  private Map<String, Integer> inputColPatternsIndex;

  private RepeatedColumnNameGenerator repeatedColNameGen;

  private ColumnShardBuilderFactory columnShardBuilderFactory;

  public RepeatedProjectStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      FunctionFactory functionFactory, ColumnShardBuilderFactory columnShardBuilderFactory,
      RepeatedColumnNameGenerator repeatedColNameGen, ColumnPatternUtil columnPatternUtil, String functionNameLowerCase,
      ColumnOrValue[] functionParameters, String outputColPattern) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.functionFactory = functionFactory;
    this.columnShardBuilderFactory = columnShardBuilderFactory;
    this.repeatedColNameGen = repeatedColNameGen;
    this.columnPatternUtil = columnPatternUtil;
    this.functionNameLowerCase = functionNameLowerCase;
    this.functionParameters = functionParameters;
    this.outputColNameBase = outputColPattern;

    // remove [*].
    if (this.outputColNameBase.endsWith(repeatedColNameGen.allEntriesIdentifyingSubstr()))
      this.outputColNameBase = this.outputColNameBase.substring(0,
          this.outputColNameBase.length() - repeatedColNameGen.allEntriesIdentifyingSubstr().length());

    inputColPatterns = Stream.of(functionParameters).filter(colOrValue -> colOrValue.getType().equals(Type.COLUMN))
        .map(colOrValue -> colOrValue.getColumnName()).collect(Collectors.toList());

    inputColPatternsIndex = new HashMap<>();
    for (int i = 0; i < inputColPatterns.size(); i++)
      inputColPatternsIndex.put(inputColPatterns.get(i), i);
  }

  @Override
  protected void execute() {
    boolean finalRun = colBuiltConsumer.getNumberOfTimesWired() == 0 || allInputColumnsBuilt.get();

    ColumnPatternContainer columnPatternContainer;
    Set<List<String>> colCombinations;
    try {
      columnPatternContainer = columnPatternUtil.findColNamesForColNamePattern(defaultEnv, inputColPatterns);
      colCombinations = columnPatternContainer.getMaximumColumnPatterns();
    } catch (LengthColumnMissingException e) {
      if (finalRun)
        throw new ExecutablePlanExecutionException("Not all 'length' columns were created. Cannot proceed.");
      return;
    }

    Set<String> allNonLengthCols = colCombinations.stream().flatMap(lst -> lst.stream()).collect(Collectors.toSet());
    boolean notAllColsAvailable = allNonLengthCols.stream().anyMatch(s -> defaultEnv.getColumnShard(s) == null);
    if (notAllColsAvailable) {
      if (finalRun)
        throw new ExecutablePlanExecutionException("Not all columns were created. Cannot proceed.");
      return;
    }

    long lowestRowIdInShard = defaultEnv.getFirstRowIdInShard();
    long numberOfRowsInShard = defaultEnv.getNumberOfRowsInShard();

    LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG); // LONG is fine for the length col, other ones are
                                                                      // specified below.

    ColumnShardBuilderManager colBuilderManager =
        columnShardBuilderFactory.createColumnShardBuilderManager(colInfo, lowestRowIdInShard);

    // name of the resulting "length" column.
    String lengthColName = repeatedColNameGen.repeatedLength(outputColNameBase);

    ColumnType inputColType = defaultEnv.getColumnType(allNonLengthCols.iterator().next());
    ProjectionFunction<Object, Object> fn =
        functionFactory.createProjectionFunction(functionNameLowerCase, inputColType);

    if (fn == null)
      throw new ExecutablePlanExecutionException(
          "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

    ColumnType outputColType = fn.getOutputType();

    // register type of output columns.
    for (String outputColName : allNonLengthCols)
      colInfo.registerColumnType(outputColName, outputColType);

    QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();

    long lastRowIdInShard = defaultEnv.getLastRowIdInShard();
    LongStream.rangeClosed(defaultEnv.getFirstRowIdInShard(), lastRowIdInShard). //
        parallel().filter(l -> (l - defaultEnv.getFirstRowIdInShard()) % BATCH_SIZE == 0).forEach(new LongConsumer() {
          @Override
          public void accept(long firstRowId) {
            QueryUuid.setCurrentThreadState(uuidState);
            try {
              // group RowIds by their colCombination (which takes into account what values the "length" columns
              // actually have per row). We can then later process rowIds which have the same column combination at the
              // same time. The default grouping algorithm we use here is based on hashing (a HashMap), so that should
              // be fine for us to use sets of lists as keys.
              long lastRowIdExcluded = Math.min(firstRowId + BATCH_SIZE, lowestRowIdInShard + numberOfRowsInShard);
              Map<Set<List<String>>, List<Long>> rowIdsGroupedByColCombination =
                  LongStream.range(firstRowId, lastRowIdExcluded).mapToObj(Long::valueOf)
                      .collect(Collectors.groupingBy(rowId -> columnPatternContainer.getColumnPatterns(rowId)));

              // work on the grouped tow IDs
              for (Entry<Set<List<String>>, List<Long>> groupedRowIdsEntry : rowIdsGroupedByColCombination.entrySet()) {
                Set<List<String>> colCombinations = groupedRowIdsEntry.getKey();
                List<Long> rowIds = groupedRowIdsEntry.getValue();

                if (colCombinations.isEmpty())
                  // skip rows that had length == 0 everywhere.
                  continue;

                // fill the resulting "length" cols. They all have the same length: For each colCombination there will
                // be one element in the output array (=output repeated column). As all rowIds here have the same
                // colCombination, they have the same length.
                for (long rowId : rowIds)
                  colBuilderManager.addValues(lengthColName, new Long[] { (long) colCombinations.size() }, rowId);

                Long[] rowIdsArray = rowIds.toArray(new Long[rowIds.size()]);

                // prepare parameters to the projection function
                Map<Integer, Object> constantFunctionParams = new HashMap<>();
                Map<Integer, List<Object>> functionParamValues = new HashMap<>();
                for (int i = 0; i < functionParameters.length; i++)
                  functionParamValues.put(i, new ArrayList<>());

                for (List<String> colCombination : colCombinations) {
                  for (int paramIdx = 0; paramIdx < functionParameters.length; paramIdx++) {
                    ColumnOrValue parameter = functionParameters[paramIdx];
                    if (parameter.getType().equals(Type.LITERAL))
                      constantFunctionParams.put(paramIdx, parameter.getValue());
                    else {
                      int patternIdx = inputColPatternsIndex.get(parameter.getColumnName());
                      String actualColName = colCombination.get(patternIdx);
                      Object values[];

                      // check if its a constant column or a standard one.
                      ConstantColumnShard constantColShard = defaultEnv.getPureConstantColumnShard(actualColName);
                      if (constantColShard != null) {
                        // Fill a full value array with the constant value, as the next colCombination probably will not
                        // have the same constant value for this paramIdx, so we force ourselves to go into "array mode"
                        // for this parameter.
                        Object constantValue = constantColShard.getValue();
                        values = (Object[]) Array.newInstance(constantValue.getClass(), rowIds.size());
                        Arrays.fill(values, constantValue);
                      } else {
                        QueryableColumnShard queryableColShard = defaultEnv.getColumnShard(actualColName);
                        values = queryableColShard.getColumnShardDictionary()
                            .decompressValues(queryableColShard.resolveColumnValueIdsForRowsFlat(rowIdsArray));
                      }

                      functionParamValues.get(paramIdx).addAll(Arrays.asList(values));
                    }
                  }
                }

                ProjectionFunction<Object, Object> fn =
                    functionFactory.createProjectionFunction(functionNameLowerCase, inputColType);
                for (Entry<Integer, Object> constantParamEntry : constantFunctionParams.entrySet())
                  fn.provideConstantParameter(constantParamEntry.getKey(), constantParamEntry.getValue());

                for (Integer normalParamIdx : Sets.difference(functionParamValues.keySet(),
                    constantFunctionParams.keySet())) {
                  List<Object> valuesList = functionParamValues.get(normalParamIdx);
                  Object[] valuesArray = valuesList.toArray(
                      (Object[]) Array.newInstance(valuesList.iterator().next().getClass(), valuesList.size()));
                  fn.provideParameter(normalParamIdx, valuesArray);
                }

                // everything prepared, execute projection function.
                Object[] functionResult = fn.execute();

                // functionResult now contains the results for each colCombination and each row. The data is though
                // "grouped" by colCombination, so now to add the result values to the colBuilderManager we have to pick
                // the right indices of the array again.

                Object[] rowValueArray = (Object[]) Array.newInstance(functionResult[0].getClass(), 1);

                // TODO if rowIds are consecutive we can add multiple rows to the ColBuilderManager here at once.
                for (int rowIdIdx = 0; rowIdIdx < rowIds.size(); rowIdIdx++) {
                  for (int colCombinationIdx = 0; colCombinationIdx < colCombinations.size(); colCombinationIdx++) {
                    String outputColName = repeatedColNameGen.repeatedAtIndex(outputColNameBase, colCombinationIdx);

                    rowValueArray[0] = functionResult[colCombinationIdx * rowIds.size() + rowIdIdx];

                    colBuilderManager.addValues(outputColName, rowValueArray, rowIds.get(rowIdIdx));
                  }
                }
              }
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        });

    QueryUuid.setCurrentThreadState(uuidState);

    // make sure every row in each column is fully filled with default values, even if the last rows of repeated columns
    // had a "length" of 0. Otherwise the latter would not be filled with default values.
    colBuilderManager.expectToFillDataUpToRow(lowestRowIdInShard + numberOfRowsInShard - 1);

    // build the cols!
    for (String newColName : colBuilderManager.getAllColumnsWithValues()) {
      if (newColName.equals(lengthColName))
        // initialize length col with "0".
        colBuilderManager.fillEmptyRowsWithValue(lengthColName, 0L);

      ColumnShard newColShard = colBuilderManager.buildAndFree(newColName);

      switch (newColShard.getColumnType()) {
      case STRING:
        defaultEnv.storeTemporaryStringColumnShard((StringColumnShard) newColShard);
        break;
      case LONG:
        defaultEnv.storeTemporaryLongColumnShard((LongColumnShard) newColShard);
        break;
      case DOUBLE:
        defaultEnv.storeTemporaryDoubleColumnShard((DoubleColumnShard) newColShard);
        break;
      }
    }

    // Inform everyone that the columns are built. Inform about the length column last!
    for (String newColName : Sets.difference(colBuilderManager.getAllColumnsWithValues(),
        new HashSet<>(Arrays.asList(lengthColName))))
      forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(newColName));

    forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(lengthColName));

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
    return Arrays.asList(colBuiltConsumer);
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "outputColPattern=" + outputColNameBase;
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // noop. If wired, thats ok, if not, thats ok too.
  }

}
