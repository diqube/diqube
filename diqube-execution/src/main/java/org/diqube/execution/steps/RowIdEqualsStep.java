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
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.types.str.dict.StringDictionary;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.VersionedExecutionEnvironment;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.DiqubeCollectors;
import org.diqube.util.HashingBatchCollector;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the inputRowIds of the rows that have a specific value in a specific column (COLUMN = VALUE) or those rowIds
 * where two columns are equal (COLUMN = COLUMN).
 * 
 * <p>
 * This step can optionally be executed on a column that still needs to be constructed. In that case, a
 * {@link ColumnBuiltConsumer} input needs to be specified which keeps this step up to date with the construction of
 * that column. If no {@link ColumnBuiltConsumer} is specified, then simply the full column is searched.
 * 
 * <p>
 * Additionally, this step can be wired to the output of another {@link RowIdConsumer} which will force this instance to
 * only take those RowIds into account that are provided by the input {@link RowIdConsumer} - effectivley building a AND
 * concatenation. In contrast to a {@link RowIdAndStep} though, the two {@link RowIdEqualsStep}s that are connected that
 * way would be executed after each other, not parallel to each other. Therefore, usually a {@link RowIdAndStep} is
 * used.
 * 
 * <p>
 * This step can be used in the non-default execution by wiring an input {@link ColumnVersionBuiltConsumer}. It will
 * then not run once-off, but continuously will run completely again based on a new
 * {@link VersionedExecutionEnvironment}. The output in that case will not be the default {@link RowIdConsumer}, but an
 * {@link OverwritingRowIdConsumer}. If no {@link ColumnVersionBuiltConsumer} is wired as input, the result will be a
 * {@link RowIdConsumer}.
 * 
 * <p>
 * Only {@link StandardColumnShard} supported.
 * 
 * <p>
 * Input: 1 optional {@link ColumnBuiltConsumer}, 1 optional {@link ColumnVersionBuiltConsumer}, 1 optional
 * {@link RowIdConsumer} <br>
 * Output: {@link RowIdConsumer}s <b>or</b> {@link OverwritingRowIdConsumer}s. The latter in case a
 * {@link ColumnVersionBuiltConsumer} is wired as input, the first otherwise.
 *
 * @author Bastian Gloeckle
 */
public class RowIdEqualsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(RowIdEqualsStep.class);

  private AtomicInteger columnsBuilt = new AtomicInteger(0);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {

    @Override
    protected void doColumnBuilt(String colName) {
      if (RowIdEqualsStep.this.colName.equals(colName)
          || (RowIdEqualsStep.this.otherColName != null && RowIdEqualsStep.this.otherColName.equals(colName)))
        columnsBuilt.incrementAndGet();
    }

    @Override
    protected void allSourcesAreDone() {
    }
  };

  private VersionedExecutionEnvironment newestVersionedEnvironment = null;
  private Object newestVersionedEnvironmentSync = new Object();

  private AbstractThreadedColumnVersionBuiltConsumer columnVersionBuiltConsumer =
      new AbstractThreadedColumnVersionBuiltConsumer(this) {

        @Override
        protected void allSourcesAreDone() {
        }

        @Override
        protected void doColumnBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds) {
          if (RowIdEqualsStep.this.colName.equals(colName)
              || (RowIdEqualsStep.this.otherColName != null && RowIdEqualsStep.this.otherColName.equals(colName))) {
            synchronized (newestVersionedEnvironmentSync) {
              if (newestVersionedEnvironment == null || env.getVersion() > newestVersionedEnvironment.getVersion())
                newestVersionedEnvironment = env;
            }
          }
        }
      };

  private AtomicBoolean rowIdSourceIsDone = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> inputRowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdEqualsStep.this.rowIdSourceIsDone.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdEqualsStep.this.inputRowIds.add(rowId);
    }
  };

  private ExecutionEnvironment defaultEnv;
  /** name of the column to search the values in */
  private String colName;
  /** Sorted values to search for. If <code>null</code>, see {@link #otherColName}. */
  private Object[] values;

  /**
   * Cached ColumnValueIds of {@link #values} for our column - this is initialized lazily because the column might not
   * have been constructed when this class is instantiated. This field will be initialized on the first call to
   * {@link #execute()}.
   */
  private Long[] columnValueIdsOfSearchedValues;

  /**
   * Only set if we're not supposed to compare the value of one col to a constant, but of one col to another col.
   * <code>null</code> if {@link #values} is set.
   */
  private String otherColName;

  /**
   * rowIds that have been reported to the {@link #rowIdConsumer} as input before. This is only maintained if
   * {@link #columnVersionBuiltConsumer} is wired (and we therefore provide {@link OverwritingRowIdConsumer} output).
   */
  private NavigableSet<Long> cachedActiveRowIds = new TreeSet<>();

  /**
   * @param sortedValues
   *          Expected to be sorted!
   */
  public RowIdEqualsStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv, String colName,
      Object[] sortedValues) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.colName = colName;
    this.values = sortedValues;
    this.otherColName = null;
  }

  /**
   */
  public RowIdEqualsStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv, String colName,
      String otherColName) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.colName = colName;
    this.otherColName = otherColName;
    this.values = null;
  }

  @Override
  public void execute() {
    ExecutionEnvironment curEnv;
    synchronized (newestVersionedEnvironmentSync) {
      curEnv = newestVersionedEnvironment;
    }
    boolean allInputColumnsFullyBuilt = (otherColName == null) ? columnsBuilt.get() == 1 : columnsBuilt.get() == 2;
    if ((colBuiltConsumer.getNumberOfTimesWired() > 0 && columnVersionBuiltConsumer.getNumberOfTimesWired() > 0
        && !allInputColumnsFullyBuilt && curEnv == null) || // both column consumer are wired, none has updates
        (colBuiltConsumer.getNumberOfTimesWired() > 0 && columnVersionBuiltConsumer.getNumberOfTimesWired() == 0
            && !allInputColumnsFullyBuilt)) // only the ColumnVersionBuilt is wired and has no updates
      // we need to wait for a column to be built but it is not yet built.
      return;

    if (curEnv == null || allInputColumnsFullyBuilt)
      curEnv = defaultEnv;
    else {
      // using a VersionedExecutionEnvironment. Check if all needed cols are available already.
      if (otherColName == null && curEnv.getColumnShard(colName) == null || //
          (otherColName != null
              && (curEnv.getColumnShard(colName) == null || curEnv.getColumnShard(otherColName) == null)))
        // at least one of the required columns is not yet available in curEnv
        return;
    }

    NavigableSet<Long> activeRowIds = null;
    if (rowIdConsumer.getNumberOfTimesWired() > 0) {
      activeRowIds = new TreeSet<>(cachedActiveRowIds);
      Long rowId;
      while ((rowId = inputRowIds.poll()) != null)
        activeRowIds.add(rowId);

      if (activeRowIds.isEmpty()) {
        if (rowIdSourceIsDone.get() && inputRowIds.isEmpty()) {
          forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
          doneProcessing();
          return;
        }
      }

      if (columnVersionBuiltConsumer.getNumberOfTimesWired() > 0)
        cachedActiveRowIds = activeRowIds;
    }

    if (curEnv.getColumnShard(colName) == null)
      throw new ExecutablePlanExecutionException("Could not find column " + colName);

    Dictionary<?> columnShardDictionary = curEnv.getPureStandardColumnShard(colName).getColumnShardDictionary();
    Collection<ColumnPage> pages = curEnv.getPureStandardColumnShard(colName).getPages().values();

    if (pages.size() > 0) {
      if (values != null) {
        // we're supposed to compare one column to constant values.

        // we cache the column Value IDs in case we're not operating on different Envs each time.
        if (columnValueIdsOfSearchedValues == null || columnVersionBuiltConsumer.getNumberOfTimesWired() > 0) {
          switch (curEnv.getColumnType(colName)) {
          case LONG:
            if (!(values instanceof Long[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-number values.");
            columnValueIdsOfSearchedValues =
                ((LongDictionary<?>) columnShardDictionary).findIdsOfValues((Long[]) values);
            break;
          case STRING:
            if (!(values instanceof String[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-string values.");
            columnValueIdsOfSearchedValues =
                ((StringDictionary<?>) columnShardDictionary).findIdsOfValues((String[]) values);
            break;
          case DOUBLE:
            if (!(values instanceof Double[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-floating point values.");
            columnValueIdsOfSearchedValues =
                ((DoubleDictionary<?>) columnShardDictionary).findIdsOfValues((Double[]) values);
            break;
          }
        }

        // TODO columnValueIdsOfSearchedValues needs to be sorted -> remove all the -1s
        rowIdEqualsConstants(curEnv, colName, columnValueIdsOfSearchedValues, pages, activeRowIds);
      } else {
        // we're supposed to compare to cols to each other.
        if (curEnv.getColumnShard(otherColName) == null)
          throw new ExecutablePlanExecutionException("Could not find column " + otherColName);

        if (!curEnv.getColumnType(colName).equals(curEnv.getColumnType(otherColName)))
          throw new ExecutablePlanExecutionException("Cannot compare column " + colName + " to column " + otherColName
              + " as they have different data types.");

        Collection<ColumnPage> pages2 = curEnv.getPureStandardColumnShard(otherColName).getPages().values();

        if (pages.size() > 0 && pages2.size() > 0) {
          NavigableMap<Long, Long> equalColumnValueIds =
              findEqualIds(curEnv.getPureStandardColumnShard(colName), curEnv.getPureStandardColumnShard(otherColName));

          rowIdEqualsOtherCol(curEnv, colName, pages, otherColName, pages2, equalColumnValueIds, activeRowIds);
        }
      }
    }

    if (columnVersionBuiltConsumer.getNumberOfTimesWired() == 0 || allInputColumnsFullyBuilt) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @SuppressWarnings("unchecked")
  private <T> NavigableMap<Long, Long> findEqualIds(ColumnShard shard1, ColumnShard shard2) {
    return ((Dictionary<T>) shard1.getColumnShardDictionary())
        .findEqualIds((Dictionary<T>) shard2.getColumnShardDictionary());
  }

  private void rowIdEqualsConstants(ExecutionEnvironment curEnv, String colName, Long[] columnValueIdsOfSearchedValues,
      Collection<ColumnPage> pages, NavigableSet<Long> activeRowIds) {
    sendRowIds(curEnv, rowIdEqualsStream(curEnv, colName, columnValueIdsOfSearchedValues, pages, activeRowIds));
  }

  /**
   * Executes a terminal operation on the given stream that will send the row IDs to all output {@link RowIdConsumer}s.
   */
  private void sendRowIds(ExecutionEnvironment curEnv, Pair<Stream<Long>, QueryUuidThreadState> rowIdStreamPair) {
    Stream<Long> stream = rowIdStreamPair.getLeft();
    QueryUuidThreadState uuidState = rowIdStreamPair.getRight();
    AtomicLong numberOfRows = new AtomicLong(0);
    stream. //
        collect(new HashingBatchCollector<Long>( // RowIds are unique, so using BatchCollector is ok.
            100, // Batch size
            len -> new Long[len], // new result array
            new Consumer<Long[]>() { // Batch-collect the row IDs
              @Override
              public void accept(Long[] t) {
                numberOfRows.addAndGet(t.length);
                QueryUuid.setCurrentThreadState(uuidState);
                try {
                  if (columnVersionBuiltConsumer.getNumberOfTimesWired() == 0)
                    forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(t));
                  else
                    forEachOutputConsumerOfType(OverwritingRowIdConsumer.class, c -> c.consume(curEnv, t));
                } finally {
                  QueryUuid.clearCurrent();
                }
              }
            }));
    QueryUuid.setCurrentThreadState(uuidState);
    logger.trace("Reported {} matching rows.", numberOfRows.get());
  }

  /**
   * Searches row IDs containing specific values.
   * 
   * @param env
   *          The current {@link ExecutionEnvironment}.
   * @param columnValueIdsOfSearchedValues
   *          The ColumnValueIDs of the searched values.
   * @param pages
   *          All pages of the column that should be inspected
   * @param activeRowIds
   *          The row IDs that are 'active' - only these row IDs will be searched for the given column value IDs. These
   *          row IDs will be used in order to reduce the number of pages that will be inspected. May be
   *          <code>null</code>.
   * 
   * @return Pair of stream of rowIds and a {@link QueryUuidThreadState}. Stream of rowIDs is where the column is equal
   *         to one of the given values. If activeRowIds is set, this returned stream contains a (non-strict) sub-set of
   *         those activeRowIds. The {@link QueryUuidThreadState} needs to be restored as soon as a terminal operation
   *         on the returned stream was executed.
   */
  private Pair<Stream<Long>, QueryUuidThreadState> rowIdEqualsStream(ExecutionEnvironment env, String colName,
      Long[] columnValueIdsOfSearchedValues, Collection<ColumnPage> pages, NavigableSet<Long> activeRowIds) {
    QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();
    return new Pair<>(pages.stream().parallel(). // stream all Pages in parallel
        filter(new Predicate<ColumnPage>() { // filter out inactive Pages
          @Override
          public boolean test(ColumnPage page) {
            QueryUuid.setCurrentThreadState(uuidState);

            try {
              if (activeRowIds != null) {
                // If we're restricting the row IDs, we check if the page contains any row that we are interested in.
                Long interestedRowId = activeRowIds.ceiling(page.getFirstRowId());
                if (interestedRowId == null || interestedRowId > page.getFirstRowId() + page.size())
                  return false;
              }

              return page.getColumnPageDict().containsAnyValue(columnValueIdsOfSearchedValues);
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        }).map(new Function<ColumnPage, Pair<ColumnPage, Set<Long>>>() { // find ColumnPageValue IDs for active
                                                                         // Pages
          @Override
          public Pair<ColumnPage, Set<Long>> apply(ColumnPage page) {
            QueryUuid.setCurrentThreadState(uuidState);
            try {
              queryRegistry.getOrCreateCurrentStatsManager().registerPageAccess(page, env.isTemporaryColumn(colName));

              Long[] pageValueIdsArray = page.getColumnPageDict().findIdsOfValues(columnValueIdsOfSearchedValues);
              Set<Long> pageValueIdsSet = new HashSet<Long>(Arrays.asList(pageValueIdsArray));
              return new Pair<>(page, pageValueIdsSet);
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        }).flatMap(new Function<Pair<ColumnPage, Set<Long>>, Stream<Long>>() { // resolve RowIDs and map them flat
          // into a single stream
          @Override
          public Stream<Long> apply(Pair<ColumnPage, Set<Long>> pagePair) {
            QueryUuid.setCurrentThreadState(uuidState);

            try {
              ColumnPage page = pagePair.getLeft();
              Set<Long> searchedPageValueIds = pagePair.getRight();
              List<Long> res = new LinkedList<>();
              if (activeRowIds != null) {
                // If we're restricted to a specific set of row IDs, we decompress only the corresponding values and
                // check those.
                SortedSet<Long> activeRowIdsInThisPage = activeRowIds.subSet( //
                    page.getFirstRowId(), page.getFirstRowId() + page.size());
                List<Integer> valueIndices = activeRowIdsInThisPage.stream()
                    .map(rowId -> (int) (rowId - page.getFirstRowId())).collect(Collectors.toList());

                List<Long> decompressedColumnPageIds = page.getValues().getMultiple(valueIndices);

                for (int i = 0; i < decompressedColumnPageIds.size(); i++) {
                  Long decompressedColumnPageId = decompressedColumnPageIds.get(i);
                  if (searchedPageValueIds.contains(decompressedColumnPageId))
                    res.add(valueIndices.get(i) + page.getFirstRowId());
                }
              } else {
                // TODO #2 STAT use statistics to decide if we should decompress the whole array here.
                long[] decompressedValues = page.getValues().decompressedArray();
                for (int i = 0; i < decompressedValues.length; i++) {
                  if (searchedPageValueIds.contains(decompressedValues[i]))
                    res.add(i + page.getFirstRowId());
                }
              }
              return res.stream();
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        }), uuidState);
  }

  /**
   * Finds row IDs where two columns have the same value.
   * 
   * @param colName1
   *          Name of the column "pages1" belongs to.
   * @param pages1
   *          All {@link ColumnPage}s of the first column.
   * @param colName2
   *          Name of the column "pages2" belongs to.
   * @param pages2
   *          All {@link ColumnPage}s of the second column.
   * @param equalColumnValueIds
   *          the result of e.g. {@link LongDictionary#findEqualIds(LongDictionary)} - keys are column value ids for
   *          pages1 mapped to the column value ids for pages2 that represent equal values.
   * @param activeRowIds
   *          If there are any, the set of row IDs that should be inspected at most.
   */
  private void rowIdEqualsOtherCol(ExecutionEnvironment curEnv, String colName1, Collection<ColumnPage> pages1,
      String colName2, Collection<ColumnPage> pages2, NavigableMap<Long, Long> equalColumnValueIds,
      NavigableSet<Long> activeRowIds) {
    Long[] columnValueIds1 = equalColumnValueIds.keySet().stream().toArray((l) -> new Long[l]);
    Long[] columnValueIds2 = equalColumnValueIds.values().stream().sorted().toArray((l) -> new Long[l]);

    // TODO #2 STAT use statistics to first search the column that produces less results.

    // first: Find rows in col1 that have the given value
    Pair<Stream<Long>, QueryUuidThreadState> stillActiveRowIdsPair =
        rowIdEqualsStream(curEnv, colName, columnValueIds1, pages1, activeRowIds);
    NavigableSet<Long> stillActiveRowIds = stillActiveRowIdsPair.getLeft().collect(DiqubeCollectors.toNavigableSet());
    QueryUuid.setCurrentThreadState(stillActiveRowIdsPair.getRight());

    // then: search in the resulting Row ID stream those rowIds that have a valid value in col2.
    Pair<Stream<Long>, QueryUuidThreadState> equalRowIds =
        rowIdEqualsStream(curEnv, otherColName, columnValueIds2, pages2, stillActiveRowIds);

    sendRowIds(curEnv, equalRowIds);
  }

  @Override
  public List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(
        Arrays.asList(new GenericConsumer[] { colBuiltConsumer, rowIdConsumer, columnVersionBuiltConsumer }));
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof RowIdConsumer)
        && !(consumer instanceof OverwritingRowIdConsumer))
      throw new IllegalArgumentException("Only RowIdConsumers and OverwritingRowIdConsumer accepted!");
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    boolean outputContainsDefault = outputConsumers.stream().anyMatch(c -> c instanceof RowIdConsumer);
    boolean outputContainsOverwriting = outputConsumers.stream().anyMatch(c -> c instanceof OverwritingRowIdConsumer);
    if (outputContainsDefault && outputContainsOverwriting)
      throw new ExecutablePlanBuildException(
          "Only either a RowIdConsumer or a OverwritingRowIdConsumer can be wired " + "as output!");
    if (columnVersionBuiltConsumer.getNumberOfTimesWired() > 0 && !outputContainsOverwriting
        || columnVersionBuiltConsumer.getNumberOfTimesWired() == 0 && !outputContainsDefault)
      throw new ExecutablePlanBuildException("If ColumnVersionBuiltConsumer is wired, the overwriting output "
          + "consumer needs to be wired, if no ColumnVersionBuiltConsumer is wired then the RowIdConsumer output "
          + "needs to be wired.");
  }

  @Override
  protected String getAdditionalToStringDetails() {
    if (values != null)
      return "colName=" + colName + ",values=" + Arrays.toString(values);
    return "colName=" + colName + ",otherColName=" + otherColName;
  }
}
