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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.function.FunctionFactory;
import org.diqube.function.ProjectionFunction;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * A step that projects values e.g. of another column.
 * 
 * <p>
 * A {@link ProjectStep} basically executes a {@link ProjectionFunction} on a specific set of input parameters and
 * creates a new column out of the results.
 * 
 * <p>
 * The resulting column is either a {@link StandardColumnShard} or a {@link ConstantColumnShard}, based on the input
 * parameters to the function: If they are only constants or constants and other {@link ConstantColumnShard}s, a
 * {@link ConstantColumnShard} will be built, otherwise a {@link StandardColumnShard} will be built.
 * 
 * 
 * <p>
 * Input: multiple optional {@link ColumnBuiltConsumer}, multiple optional {@link ColumnVersionBuiltConsumer}. <br>
 * Output: {@link ColumnBuiltConsumer}, {@link ColumnVersionBuiltConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class ProjectStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(ProjectStep.class);

  /** true as soon as input ColumnBuiltConsumer has reported "done" */
  private AtomicBoolean inputSourcesDone = new AtomicBoolean(false);

  /** only important if a ColumnBuiltConsumer is wired, contains those columns that have not yet been built fully. */
  private Set<String> columnsThatStillNeedToBeBuilt;
  /** True as soon as all columns that this projectstep relies on are built. */
  private AtomicBoolean allColumnsBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer columnBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void doColumnBuilt(String colName) {
      columnsThatStillNeedToBeBuilt.remove(colName);

      if (columnsThatStillNeedToBeBuilt.isEmpty())
        allColumnsBuilt.set(true);
    }

    @Override
    protected void allSourcesAreDone() {
      inputSourcesDone.set(true);
    }
  };

  private Object newestSync = new Object();
  /**
   * Newest version of {@link VersionedExecutionEnvironment} that should be used to resolve any values while being based
   * on intermediary columns (= happens only on query master)!. Sync access using {@link #newestSync}.
   */
  private VersionedExecutionEnvironment newestTemporaryEnv = null;
  /**
   * The rowIds that have been reported as being "adjusted" since the last run of #execute(). "Adjusted" means that the
   * values of these rowIds might have changed. Sync access using {@link #newestSync}.
   */
  private Set<Long> newestAdjustedRowIds = new HashSet<>();

  private AbstractThreadedColumnVersionBuiltConsumer columnVersionBuiltConsumer =
      new AbstractThreadedColumnVersionBuiltConsumer(this) {
        @Override
        protected void allSourcesAreDone() {
          // we rely on ColumnBuiltConsumer to report the final build.
        }

        @Override
        protected void doColumnBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds) {
          synchronized (newestSync) {
            if (newestTemporaryEnv == null)
              newestTemporaryEnv = env;
            else if (newestTemporaryEnv.getVersion() < env.getVersion())
              newestTemporaryEnv = env;
            newestAdjustedRowIds.addAll(adjustedRowIds);
          }
        }
      };

  private ExecutionEnvironment defaultEnv;
  /** Output projected values to this column */
  private String outputColName;
  private FunctionFactory functionFactory;
  /** parameters to pass to the {@link ProjectionFunction}. */
  private ColumnOrValue[] functionParameters;
  /** function name of the function to execute */
  private String functionNameLowerCase;
  /**
   * Prepared set containing the names of the columns that show up in the input parameters of the function. Having a
   * column name in here means that the execution of the {@link ProjectionFunction} depends on this column being
   * available.
   */
  private Set<String> inputColNames;

  private Function<ColumnType, ColumnShardBuilderManager> columnShardBuilderManagerSupplier;
  private ColumnVersionManager columnVersionManager;
  private ColumnShardFactory columnShardFactory;
  private ColumnShardBuilderFactory columnShardBuilderFactory;

  /**
   * @param functionNameLowerCase
   *          name of the function to be executed
   * @param functionParameters
   *          The parameters
   * @param outputColName
   *          column to be created.
   * @param columnShardBuilderFactory
   *          factory for creating a new col.
   * @param columnVersionManager
   *          Needed in case {@link ColumnVersionBuiltConsumer} are wired and intermediate columns should be created.
   *          This is needed on query master only.
   */
  public ProjectStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      FunctionFactory functionFactory, String functionNameLowerCase, ColumnOrValue[] functionParameters,
      String outputColName, ColumnShardBuilderFactory columnShardBuilderFactory, ColumnShardFactory columnShardFactory,
      ColumnVersionManager columnVersionManager) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.functionFactory = functionFactory;
    this.functionNameLowerCase = functionNameLowerCase;
    this.functionParameters = functionParameters;
    this.outputColName = outputColName;
    this.columnShardBuilderFactory = columnShardBuilderFactory;
    this.columnShardFactory = columnShardFactory;
    this.columnVersionManager = columnVersionManager;
  }

  @Override
  public void initialize() {
    inputColNames = new HashSet<>();
    for (ColumnOrValue param : functionParameters)
      if (param.getType().equals(ColumnOrValue.Type.COLUMN))
        inputColNames.add(param.getColumnName());

    columnsThatStillNeedToBeBuilt = new ConcurrentSkipListSet<>(inputColNames);
    for (Iterator<String> it = columnsThatStillNeedToBeBuilt.iterator(); it.hasNext();)
      if (defaultEnv.getColumnShard(it.next()) != null)
        it.remove();

    columnShardBuilderManagerSupplier = (outputColType) -> {
      LoaderColumnInfo columnInfo = new LoaderColumnInfo(outputColType);
      return columnShardBuilderFactory.createColumnShardBuilderManager(columnInfo, defaultEnv.getFirstRowIdInShard());
    };
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof ColumnBuiltConsumer)
        && !(consumer instanceof ColumnVersionBuiltConsumer))
      throw new IllegalArgumentException("Only ColumnBuiltConsumer and ColumnVersionBuiltConsumer supported.");
  }

  @Override
  protected void execute() {
    // Did we fill the output column completely and are we done?
    boolean columnFullyBuilt = false;

    ColumnShard column = null;

    VersionedExecutionEnvironment temporaryEnv;
    Set<Long> curAdjustedRowIds;
    synchronized (newestSync) {
      temporaryEnv = newestTemporaryEnv;
      curAdjustedRowIds = newestAdjustedRowIds;
      if (curAdjustedRowIds != null && !curAdjustedRowIds.isEmpty())
        newestAdjustedRowIds = new HashSet<>();
    }

    if (inputColNames.size() == 0) {
      // we do not have input columns, just literals. The resulting column will likely end up being a column with only
      // one row, a 'constant' row. This is handled accordingly in ResolveColumnDictIdsStep.

      ColumnType inputColType = null;
      if (functionParameters[0].getValue() instanceof Long)
        inputColType = ColumnType.LONG;
      else if (functionParameters[0].getValue() instanceof String)
        inputColType = ColumnType.STRING;
      else if (functionParameters[0].getValue() instanceof Double)
        inputColType = ColumnType.DOUBLE;

      ProjectionFunction<Object, Object> fn =
          functionFactory.createProjectionFunction(functionNameLowerCase, inputColType);

      if (fn == null)
        throw new ExecutablePlanExecutionException(
            "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColType);

      for (int paramIdx = 0; paramIdx < functionParameters.length; paramIdx++)
        fn.provideConstantParameter(paramIdx, functionParameters[paramIdx].getValue());

      Object[] fnResult = fn.execute();

      switch (fn.getOutputType()) {
      case LONG:
        column = columnShardFactory.createConstantLongColumnShard(outputColName, (Long) fnResult[0],
            defaultEnv.getFirstRowIdInShard());
        break;
      case STRING:
        column = columnShardFactory.createConstantStringColumnShard(outputColName, (String) fnResult[0],
            defaultEnv.getFirstRowIdInShard());
        break;
      case DOUBLE:
        column = columnShardFactory.createConstantDoubleColumnShard(outputColName, (Double) fnResult[0],
            defaultEnv.getFirstRowIdInShard());
        break;
      }
      columnFullyBuilt = true;
      logger.trace("Build constant column {} as there are no column inputs. Value: {}", outputColName, fnResult[0]);
    } else if (columnBuiltConsumer.getNumberOfTimesWired() == 0
        || (columnBuiltConsumer.getNumberOfTimesWired() > 0 && allColumnsBuilt.get())) {
      // We waited enough, all our source columns are built fully and are available in the defaultEnv.

      logger.trace("Build standard column {} based on default environment (= last run).", outputColName);
      column = buildColumnBasedProjection(defaultEnv);
      columnFullyBuilt = true;
    } else if (columnBuiltConsumer.getNumberOfTimesWired() > 0 && inputSourcesDone.get() && !allColumnsBuilt.get()) {
      // we need to wait for columns to be built, but the columnBuiltConsumer reported to be done, but not all columns
      // have been built. Therefore we cannot execute the projection, but just report "done".
      logger.debug("Projection waited for columns to be built, but some won't be built. Skipping.");
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
      return;
    } else {
      // not all columns are yet fully available. Let's see if we have enough information to at least project some parts
      // for the time being.

      if (temporaryEnv != null && existsOutputConsumerOfType(ColumnVersionBuiltConsumer.class)) {
        boolean allInputColsAvailable =
            inputColNames.stream().allMatch(colName -> temporaryEnv.getColumnShard(colName) != null);

        if (allInputColsAvailable) {
          // we have data for all input columns available, which means that we can start projection at least
          // /something/.

          logger.trace("Build intermediary column {} after following rowIds were adjusted (limit) {}", outputColName,
              Iterables.limit(curAdjustedRowIds, 100));

          // execute full projection, although we have specific row IDs that have been altered.
          // TODO #8 cache intermediary results and use that to not again apply the projection function to all elements
          // again.
          column = buildColumnBasedProjection(temporaryEnv);
        }
      }
    }

    if (column != null) {
      if (temporaryEnv != null && columnVersionManager != null
          && existsOutputConsumerOfType(ColumnVersionBuiltConsumer.class)) {
        logger.trace("Will store new version of {}", outputColName);
        // inform ColumnVersionBuiltConsumer
        VersionedExecutionEnvironment newEnv = columnVersionManager.createNewVersion(column);

        forEachOutputConsumerOfType(ColumnVersionBuiltConsumer.class,
            c -> c.columnVersionBuilt(newEnv, outputColName, curAdjustedRowIds));
      }

      // if done, inform other consumers.
      if (columnFullyBuilt) {
        logger.trace("Will store final column {}", outputColName);

        switch (column.getColumnType()) {
        case STRING:
          defaultEnv.storeTemporaryStringColumnShard((StringColumnShard) column);
          break;
        case LONG:
          defaultEnv.storeTemporaryLongColumnShard((LongColumnShard) column);
          break;
        case DOUBLE:
          defaultEnv.storeTemporaryDoubleColumnShard((DoubleColumnShard) column);
          break;
        }
        forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(outputColName));
        forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
        doneProcessing();
      }
    }
  }

  /**
   * Executes a projection that is based on at least one {@link ColumnShard}, which is/are available in the given
   * {@link ExecutionEnvironment}.
   * 
   * The created column, which might either be a {@link StandardColumnShard} or a {@link ConstantColumnShard} (in case
   */
  private ColumnShard buildColumnBasedProjection(ExecutionEnvironment env) {

    // buckets of row IDs we want to process together. Left of pair: first row ID of bucket, right: length.
    Set<Pair<Long, Integer>> rowIdBucketsToProcess;

    if (inputColNames.stream().anyMatch(colName -> env.getPureStandardColumnShard(colName) != null)) {
      // Find column shard that contains the least rows, in order to calculate rowID buckets below.
      // On the query master each column might have different number of rows, therefore we find the least common number
      // of rows that we can process.
      String referenceColName =
          inputColNames.stream().filter(colName -> env.getPureStandardColumnShard(colName) != null).map(name -> //
          new Pair<String, Long>(name, env.getPureStandardColumnShard(name).getNumberOfRowsInColumnShard()))
              .min((p1, p2) -> p1.getRight().compareTo(p2.getRight())).get().getLeft();

      rowIdBucketsToProcess = env.getColumnShard(referenceColName).getGoodResolutionPairs();
    } else {
      // only ConstantColumnShard objects.
      rowIdBucketsToProcess = new HashSet<Pair<Long, Integer>>();
      rowIdBucketsToProcess.add(new Pair<Long, Integer>(defaultEnv.getFirstRowIdInShard(), 1));
    }

    // choose an arbitrary input column to identify input colType. All input columns and constants need to be of equal
    // type anyway.
    ColumnType inputColumnType = env.getColumnType(inputColNames.stream().findAny().get());

    ConstantColumnShard[] resultConstantColumn = new ConstantColumnShard[1];
    resultConstantColumn[0] = null;

    ProjectionFunction<?, ?> tmpProjectionFunction =
        functionFactory.createProjectionFunction(functionNameLowerCase, inputColumnType);

    if (tmpProjectionFunction == null)
      throw new ExecutablePlanExecutionException(
          "Cannot find function '" + functionNameLowerCase + "' with input data type " + inputColumnType);

    ColumnShardBuilderManager columnShardBuilderManager =
        columnShardBuilderManagerSupplier.apply(tmpProjectionFunction.getOutputType());

    // execute ProjectionFunctions based on buckets of rowIds.
    rowIdBucketsToProcess.forEach(new Consumer<Pair<Long, Integer>>() {
      @Override
      public void accept(Pair<Long, Integer> pair) {
        long firstRowId = pair.getLeft();
        int length = pair.getRight();

        ProjectionFunction<Object, Object> fn =
            functionFactory.createProjectionFunction(functionNameLowerCase, inputColumnType);

        boolean hadStandardColumnInput = false;
        for (int paramIdx = 0; paramIdx < functionParameters.length; paramIdx++) {
          ColumnOrValue param = functionParameters[paramIdx];
          if (param.getType() == ColumnOrValue.Type.LITERAL) {
            fn.provideConstantParameter(paramIdx, param.getValue());
          } else {
            ConstantColumnShard constantShard = env.getPureConstantColumnShard(param.getColumnName());
            if (constantShard != null) {
              fn.provideConstantParameter(paramIdx, constantShard.getValue());
            } else {
              hadStandardColumnInput = true;
              Object[] colValues = fn.createEmptyInputArray(length);
              int rowsResolved =
                  resolveValuesFromColumn(env.getColumnShard(param.getColumnName()), firstRowId, length, colValues);
              if (rowsResolved != length)
                throw new ExecutablePlanExecutionException("Column " + param.getColumnName()
                    + " does not contain the same number of rows as other columns; cannot execute function "
                    + functionNameLowerCase + " to produce output column " + outputColName);
              fn.provideParameter(paramIdx, colValues);
            }
          }
        }

        Object[] fnResult = fn.execute();

        if (hadStandardColumnInput) {
          columnShardBuilderManager.addValues(outputColName, fnResult, firstRowId);
        } else {
          // we did not have input from a standardColumnShard. We would not execute this method if there were no
          // column
          // input at all, therefore all column inputs were constantColumnShards. Because of this we should again
          // build
          // a constantColumnShard.
          // It is no problem to directly create the result column within the forEach(..) call, as in case all inputs
          // are constants, there is only one Pair<Long, Integer> the forEach is iterating over.
          switch (fn.getOutputType()) {
          case LONG:
            resultConstantColumn[0] = columnShardFactory.createConstantLongColumnShard(outputColName,
                (Long) fnResult[0], defaultEnv.getFirstRowIdInShard());
            break;
          case STRING:
            resultConstantColumn[0] = columnShardFactory.createConstantStringColumnShard(outputColName,
                (String) fnResult[0], defaultEnv.getFirstRowIdInShard());
            break;
          case DOUBLE:
            resultConstantColumn[0] = columnShardFactory.createConstantDoubleColumnShard(outputColName,
                (Double) fnResult[0], defaultEnv.getFirstRowIdInShard());
            break;
          }
        }
      }
    });

    if (resultConstantColumn[0] == null)
      return columnShardBuilderManager.buildAndFree(outputColName);

    return resultConstantColumn[0];
  }

  /**
   * Resolves values of a specific row ID range from the given column and takes care of fetching those values from the
   * {@link ColumnPage}s that contain them.
   *
   * @return number of elements resolved - this might be smaller than 'length' in case source column did not provide
   *         enough data.
   */
  private int resolveValuesFromColumn(QueryableColumnShard column, long firstRowId, int length, Object[] result) {
    if (column.getFirstRowId() > firstRowId) {
      // make sure firstRowId is inside the column shard.
      long delta = firstRowId - column.getFirstRowId();
      length -= delta;
      firstRowId += delta;
      if (length <= 0)
        return 0;
    }

    Long[] columnValueIds = column.resolveColumnValueIdsForRowsFlat( //
        LongStream.range(firstRowId, firstRowId + length).mapToObj(Long::valueOf).toArray(l -> new Long[l]));

    int resultIdx = 0;
    for (long colValueId : columnValueIds) {
      // resolveColumnValueIdsForRowsFlat returns -1 for rowIds not contained in the column shard. This can happen if
      // the length parameter of this method is too high. As we though provided a sorted input to
      // resolveColumnValueIdsForRowsFlat, this can happen only at the end of the columnValueIds array, therefore we
      // just have to 'break'.
      if (colValueId == -1L)
        break;

      result[resultIdx++] = column.getColumnShardDictionary().decompressValue(colValueId);
    }

    return resultIdx;
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { columnBuiltConsumer, columnVersionBuiltConsumer }));
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // noop. Both is fine, having an input and not having an input.
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "funcName=" + functionNameLowerCase + ", outputCol=" + outputColName;
  }

}
