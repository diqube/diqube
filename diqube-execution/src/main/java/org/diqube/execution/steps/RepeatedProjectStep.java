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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
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
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.queries.QueryRegistry;
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
 * This step is currently pretty expensive, as it calculates the results for each row in the input {@link TableShard}
 * separately.
 * 
 * <p>
 * Input: multiple optional {@link ColumnBuiltConsumer} <br>
 * Output: {@link ColumnBuiltConsumer} (the resulting "length" column will be published last to this consumer).
 *
 * @author Bastian Gloeckle
 */
public class RepeatedProjectStep extends AbstractThreadedExecutablePlanStep {
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

    long lowestRowId = defaultEnv.getTableShardIfAvailable().getLowestRowId();
    long numberOfRows = defaultEnv.getTableShardIfAvailable().getNumberOfRowsInShard();

    ColumnType inputColType = null;
    ColumnType outputColType = null;

    LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG); // LONG is fine for the length col, other ones are
                                                                      // specified below.

    ColumnShardBuilderManager colBuilderManager =
        columnShardBuilderFactory.createColumnShardBuilderManager(colInfo, lowestRowId);

    // name of the resulting "length" column.
    String lengthColName = repeatedColNameGen.repeatedLength(outputColNameBase);

    // process each input row by itself. This is expensive, but as each row might have different "lengths" of the input
    // repeated cols, this is the cleanest way to implement this.
    for (long rowId = lowestRowId; rowId < lowestRowId + numberOfRows; rowId++) {
      long finalRowId = rowId;
      // Find the input column index combinations that are valid in this row. For each of the returned lists, one result
      // column will be built, all of those columns are a repeated field.
      Set<List<String>> colCombinationsForRow = columnPatternContainer.getColumnPatterns(finalRowId);

      // fill the resulting "length" col.
      colBuilderManager.addValues(lengthColName, new Long[] { (long) colCombinationsForRow.size() }, rowId);

      Iterator<List<String>> colCombIt = colCombinationsForRow.iterator();
      for (int colCombinationIdx = 0; colCombinationIdx < colCombinationsForRow.size(); colCombinationIdx++) {
        List<String> colCombination = colCombIt.next();

        if (inputColType == null)
          inputColType = defaultEnv.getColumnType(colCombination.get(0));

        ProjectionFunction<Object, Object> fn =
            functionFactory.createProjectionFunction(functionNameLowerCase, inputColType);

        if (fn == null)
          throw new ExecutablePlanExecutionException(
              "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

        if (outputColType == null)
          outputColType = fn.getOutputType();

        for (int paramIdx = 0; paramIdx < functionParameters.length; paramIdx++) {
          ColumnOrValue parameter = functionParameters[paramIdx];
          Object value;

          if (parameter.getType().equals(Type.LITERAL))
            value = parameter.getValue();
          else {
            int patternIdx = inputColPatternsIndex.get(parameter.getColumnName());
            String actualColName = colCombination.get(patternIdx);
            ColumnShard colShard = defaultEnv.getPureConstantColumnShard(actualColName);

            if (colShard != null)
              value = ((ConstantColumnShard) colShard).getValue();
            else {
              QueryableColumnShard queryableColShard = defaultEnv.getColumnShard(actualColName);
              value = queryableColShard.getColumnShardDictionary()
                  .decompressValue(queryableColShard.resolveColumnValueIdForRow(finalRowId));
            }
          }
          fn.provideConstantParameter(paramIdx, value);
        }

        Object[] functionResult = fn.execute(); // single entry array as we provided only constant parameters.

        String outputColName = repeatedColNameGen.repeatedAtIndex(outputColNameBase, colCombinationIdx);

        colInfo.registerColumnType(outputColName, outputColType);
        colBuilderManager.addValues(outputColName, functionResult, rowId);
      }
    }

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
