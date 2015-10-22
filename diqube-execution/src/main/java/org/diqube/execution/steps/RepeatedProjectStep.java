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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.ConstantColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.cache.ColumnShardCache;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>
 * This step does an extensive inspection in what output columns are available in the {@link ExecutionEnvironment}
 * already (i.e. are in the {@link ColumnShardCache}) and will create only those columns that are not in the cache.
 * 
 * <p>
 * Input: multiple optional {@link ColumnBuiltConsumer} <br>
 * Output: {@link ColumnBuiltConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class RepeatedProjectStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(RepeatedProjectStep.class);

  private static final int BATCH_SIZE = ColumnShardBuilder.PROPOSAL_ROWS; // approx work on whole ColumnPages.

  private static final Comparator<List<String>> LIST_COMPARATOR = new Comparator<List<String>>() {
    @Override
    public int compare(List<String> o1, List<String> o2) {
      if (o1 == null && o2 == null)
        return 0;
      if (o1 == null ^ o2 == null)
        return (o1 == null) ? -1 : 1; // sort nulls before anything else.

      Iterator<String> i1 = o1.iterator();
      Iterator<String> i2 = o2.iterator();

      while (i1.hasNext()) {
        String next1 = i1.next();
        if (!i2.hasNext())
          return 1; // o1 > o2, because o1 longer than o2 & all of o2 elements match o1.
        String next2 = i2.next();
        int cmp = next1.compareTo(next2);
        if (cmp != 0)
          return cmp;
      }

      if (i2.hasNext())
        return -1; // o1 < o2, because o2 longer than o1 & all of o1 elements match o2.
      return 0;
    }
  };

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
  }

  @Override
  public void initialize() {
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
    boolean lengthColumnIsCached = defaultEnv.getColumnShard(lengthColName) != null;

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
              logger.trace("Executing batch {}", firstRowId);

              // group RowIds by their colCombination (which takes into account what values the "length" columns
              // actually have per row). We can then later process rowIds which have the same column combination at the
              // same time. The default grouping algorithm we use here is based on hashing (a HashMap), so that should
              // be fine for us to use sets of lists as keys.
              long lastRowIdExcluded = Math.min(firstRowId + BATCH_SIZE, lowestRowIdInShard + numberOfRowsInShard);
              Map<Set<List<String>>, List<Long>> rowIdsGroupedByColCombination =
                  LongStream.range(firstRowId, lastRowIdExcluded).mapToObj(Long::valueOf)
                      .collect(Collectors.groupingBy(rowId -> columnPatternContainer.getColumnPatterns(rowId)));

              logger.trace("Found {} groups for batch {}", rowIdsGroupedByColCombination.size(), firstRowId);

              // work on the grouped row IDs
              for (Entry<Set<List<String>>, List<Long>> groupedRowIdsEntry : rowIdsGroupedByColCombination.entrySet()) {
                Set<List<String>> colCombinationsSet = groupedRowIdsEntry.getKey();
                List<Long> rowIds = groupedRowIdsEntry.getValue();

                if (colCombinationsSet.isEmpty())
                  // skip rows that had length == 0 everywhere.
                  continue;

                // sort col combinations and only work on the sorted set. This is needed, as the index in the
                // colcombination list corresponds to the index in the output column. And of that output column, several
                // indices might be cached already, so we need to make sure to traverse the col combinations in the same
                // order as before.
                List<List<String>> colCombinations =
                    colCombinationsSet.stream().sorted(LIST_COMPARATOR).collect(Collectors.toList());

                logger.trace("Working on col combinations {}", colCombinations);

                // fill the resulting "length" cols. They all have the same length: For each colCombination there will
                // be one element in the output array (=output repeated column). As all rowIds here have the same
                // colCombination, they have the same length.
                if (!lengthColumnIsCached) { // length col might be cached already.
                  logger.trace("Filling length outcol for batch {}", firstRowId);
                  for (long rowId : rowIds)
                    colBuilderManager.addValues(lengthColName, new Long[] { (long) colCombinations.size() }, rowId);
                } else
                  logger.trace("No need to fill length outcol for batch {} as it exists already.", firstRowId);

                // prepare parameters to the projection function
                Map<Integer, Object> constantFunctionParams = new HashMap<>();
                Map<Integer, List<Object>> functionParamValues = new HashMap<>();
                for (int i = 0; i < functionParameters.length; i++)
                  functionParamValues.put(i, new ArrayList<>());

                Set<Integer> skipColCombinationIndices = new HashSet<>(
                    findAllColumnCombinationIndicesToSkip(colCombinations.size()).headSet(colCombinations.size()));

                logger.info("Will skip {} out of {} col combinations on batch {}", skipColCombinationIndices.size(),
                    colCombinations.size(), firstRowId);

                if (skipColCombinationIndices.size() == colCombinations.size())
                  // we skip all col combinations. continue with next grouped rowId set!
                  continue;

                for (int colCombinationIdx = 0; colCombinationIdx < colCombinations.size(); colCombinationIdx++) {
                  if (skipColCombinationIndices.contains(colCombinationIdx))
                    // we skip this colCombination because the corresponding output column is available already.
                    continue;

                  List<String> colCombination = colCombinations.get(colCombinationIdx);

                  logger.trace("Working on col combination {} for batch {}", colCombination, firstRowId);

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
                            .decompressValues(queryableColShard.resolveColumnValueIdsForRowsFlat(rowIds));
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

                logger.trace("Prepared params to projection function in batch {}, will execute it now!", firstRowId);

                // everything prepared, execute projection function.
                Object[] functionResult = fn.execute();

                // functionResult now contains the results for each colCombination (nut not the skipped ones!) and each
                // row. The data is though "grouped" by colCombination, so now to add the result values to the
                // colBuilderManager we have to pick the right indices of the array again.

                Object[] rowValueArray = (Object[]) Array.newInstance(functionResult[0].getClass(), 1);

                logger.trace("Function executed, putting results into output columns for batch {}", firstRowId);

                // TODO if rowIds are consecutive we can add multiple rows to the ColBuilderManager here at once.
                for (int rowIdIdx = 0; rowIdIdx < rowIds.size(); rowIdIdx++) {
                  int cleanColCombinationIndex = 0; // colCombinationIndex not counting those colCombinations that have
                                                    // been skipped.

                  for (int colCombinationIdx = 0; colCombinationIdx < colCombinations.size(); colCombinationIdx++) {
                    if (skipColCombinationIndices.contains(colCombinationIdx))
                      // we did not produce results for this colCombination!
                      continue;

                    String outputColName = repeatedColNameGen.repeatedAtIndex(outputColNameBase, colCombinationIdx);

                    rowValueArray[0] = functionResult[cleanColCombinationIndex * rowIds.size() + rowIdIdx];
                    colBuilderManager.addValues(outputColName, rowValueArray, rowIds.get(rowIdIdx));
                    cleanColCombinationIndex++;
                  }
                }
              }
            } finally {
              QueryUuid.clearCurrent();
            }
          }

          private ConcurrentSkipListSet<Integer> indicesToSkip = new ConcurrentSkipListSet<>();
          private AtomicInteger indicesToSkipMaxCheckedLength = new AtomicInteger(-1);

          /**
           * @return The indices in ColCombinations to be skipped, because the output columns already exist. This is
           *         thread safe. This method re-uses results from previous calls. It is guaraneteed that for a given
           *         maxNumber the set of indices to skip in the range 0..(maxNumber - 1) will not change even if other
           *         threads call this method with a different parameter.
           */
          private NavigableSet<Integer> findAllColumnCombinationIndicesToSkip(int maxNumberOfColCombinations) {
            int previousMaxLength = indicesToSkipMaxCheckedLength.get();
            if (maxNumberOfColCombinations <= previousMaxLength)
              return indicesToSkip;

            // calculate new skipped indices before updating indicesToSkipMaxCheckedIndex -> as soon as
            // indicesToSkipMaxCheckedIndex is updated, the values in indicesToSkip are valid for that max index. If
            // we're unfortunate, we might calculate the skipped indices twice, though, but that is not as bad.
            for (int idx = previousMaxLength; idx < maxNumberOfColCombinations; idx++) {
              String colName = repeatedColNameGen.repeatedAtIndex(outputColNameBase, idx);
              if (defaultEnv.getColumnShard(colName) != null)
                indicesToSkip.add(idx);
            }

            indicesToSkipMaxCheckedLength.getAndUpdate(previous -> Math.max(previous, maxNumberOfColCombinations));
            return indicesToSkip;
          }
        });

    QueryUuid.setCurrentThreadState(uuidState);

    // make sure every row in each column is fully filled with default values, even if the last rows of repeated columns
    // had a "length" of 0. Otherwise the latter would not be filled with default values.
    colBuilderManager.expectToFillDataUpToRow(lowestRowIdInShard + numberOfRowsInShard - 1);

    logger.trace("Creating output columns: {}", colBuilderManager.getAllColumnsWithValues());

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

    logger.trace("Output columns created");

    // Inform everyone that the columns are built. Inform about the length column last!
    for (String newColName : Sets.difference(colBuilderManager.getAllColumnsWithValues(),
        new HashSet<>(Arrays.asList(lengthColName))))
      forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(newColName));

    if (!lengthColumnIsCached)
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
