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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.diqube.data.Dictionary;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.HashingBatchCollector;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the inputRowIds of the rows that have a specific inequality relation to a specific value in a specific column.
 * 
 * <p>
 * This includes the inequality operators >, >=, <, <=.
 * 
 * <p> This step can optionally be executed on a column that still needs to be constructed. In that case, a
 * {@link ColumnBuiltConsumer} input needs to be specified which keeps this step up to date with the construction of that
 * column. If no {@link ColumnBuiltConsumer} is specified, then simply the full column is searched in defaulEnv.
 * 
 * <p> Additionally, this step can be wired to the output of another {@link RowIdConsumer} which will force this
 * instance to only take those RowIds into account that are provided by the input {@link RowIdConsumer} - effectively
 * building a AND concatenation. In contrast to a {@link RowIdAndStep} though, the two {@link RowIdInequalStep}s that
 * are connected that way would be executed after each other, not parallel to each other. Therefore, usually a
 * {@link RowIdAndStep} is used.
 * 
 * <p> This step can be used in the non-default execution by wiring an input {@link ColumnVersionBuiltConsumer}. It will
 * then not run once-off, but continuously will run completely again based on a new
 * {@link VersionedExecutionEnvironment}. The output in that case will not be the default {@link RowIdConsumer}, but an
 * {@link OverwritingRowIdConsumer}. If no {@link ColumnVersionBuiltConsumer} is wired as input, the result will be a
 * {@link RowIdConsumer}.
 * 
 * <p> Only {@link StandardColumnShard}s supported.
 * 
 * <p> Input: 1 optional {@link ColumnBuiltConsumer}, 1 optional {@link ColumnVersionBuiltConsumer}, 1 optional
 * {@link RowIdConsumer} <br> Output: {@link RowIdConsumer}s or {@link OverwritingRowIdConsumer}s (see above).
 *
 * @author Bastian Gloeckle
 */
public class RowIdInequalStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(RowIdInequalStep.class);

  private AtomicInteger columnsBuilt = new AtomicInteger(0);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {

    @Override
    protected void doColumnBuilt(String colName) {
      if (RowIdInequalStep.this.colName.equals(colName)
          || (RowIdInequalStep.this.otherColName != null && RowIdInequalStep.this.otherColName.equals(colName)))
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
          if (RowIdInequalStep.this.colName.equals(colName)
              || (RowIdInequalStep.this.otherColName != null && RowIdInequalStep.this.otherColName.equals(colName))) {
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
      RowIdInequalStep.this.rowIdSourceIsDone.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdInequalStep.this.inputRowIds.add(rowId);
    }
  };

  private ExecutionEnvironment defaultEnv;
  /** name of the column to search the values in */
  private String colName;

  /**
   * Only set if we're not supposed to compare the value of one col to a constant, but of one col to another col.
   * <code>null</code> if {@link #value} is set.
   */
  private String otherColName;

  /**
   * Only set if we should compare the values of one column to a constant value. <code>null</code> if
   * {@link #otherColName} is set.
   */
  private Object value;

  /**
   * The comparator of the requested inequality operation.
   */
  private RowIdComparator comparator;

  /**
   * rowIds that have been reported to the {@link #rowIdConsumer} as input before. This is only maintained if
   * {@link #columnVersionBuiltConsumer} is wired (and we therefore provide {@link OverwritingRowIdConsumer} output).
   */
  private NavigableSet<Long> cachedActiveRowIds = new TreeSet<>();

  /**
   * The left operand to the comparison will always be the column, the right operand the constant.
   * 
   * @param value
   *          The constant to compare to.
   * @param comparator
   *          Freshly created instance of an implementation of {@link RowIdComparator}. If this step should compare with
   *          > {@link GtRowIdComparator}, if >= then {@link GtEqRowIdComparator}, if < {@link LtRowIdComparator}, if <=
   *          {@link LtEqRowIdComparator}.
   */
  public RowIdInequalStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv, String colName,
      Object value, RowIdComparator comparator) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.colName = colName;
    this.value = value;
    this.comparator = comparator;
    this.otherColName = null;
  }

  /**
   * 
   * @param comparator
   *          Freshly created instance of an implementation of {@link RowIdComparator}. If this step should compare with
   *          > {@link GtRowIdComparator}, if >= then {@link GtEqRowIdComparator}, if < {@link LtRowIdComparator}, if <=
   *          {@link LtEqRowIdComparator}.
   * @param otherColNameUsed
   *          provide true always. Needed because constructor is overloaded.
   */
  public RowIdInequalStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment env, String colName,
      String otherColName, RowIdComparator comparator, boolean otherColNameUsed) {
    super(stepId, queryRegistry);
    this.defaultEnv = env;
    this.colName = colName;
    this.otherColName = otherColName;
    this.comparator = comparator;
    this.value = null;
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

    StandardColumnShard columnShard = curEnv.getPureStandardColumnShard(colName);
    NavigableMap<Long, ColumnPage> pages = columnShard.getPages();

    if (pages.size() > 0) {
      if (value != null) {
        // we're supposed to compare one column to constant values.

        compareToConstant(curEnv, value, columnShard, activeRowIds, comparator);
      } else {
        // we're supposed to compare to cols to each other.
        if (curEnv.getColumnShard(otherColName) == null)
          throw new ExecutablePlanExecutionException("Could not find column " + otherColName);

        if (!curEnv.getColumnType(colName).equals(curEnv.getColumnType(otherColName)))
          throw new ExecutablePlanExecutionException("Cannot compare column " + colName + " to column " + otherColName
              + " as they have different data types.");

        QueryableColumnShard otherColumnShard = curEnv.getColumnShard(otherColName);

        if (((StandardColumnShard) otherColumnShard.getDelegate()).getPages().size() > 0)
          executeOnOtherCol(curEnv, columnShard, otherColumnShard, activeRowIds, comparator);
      }
    }

    if (columnVersionBuiltConsumer.getNumberOfTimesWired() == 0 || allInputColumnsFullyBuilt) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  /**
   * Executes the comparison of the given column to a constant value using the given comparator. The left operand to the
   * comparison will always be the column, the right operand the constant.
   * 
   * @param constantValue
   *          The value to compare to.
   * @param column
   *          The column whose values should be compared to the constant value.
   * @param activeRowIds
   *          Those rowIds that we should take into account for searching. May be <code>null</code> in which case we'll
   *          search all rows.
   * @param comparator
   *          The comparator implementing >, >=, < and <=.
   */
  private void compareToConstant(ExecutionEnvironment curEnv, Object constantValue, StandardColumnShard column,
      NavigableSet<Long> activeRowIds, RowIdComparator comparator) {
    sendRowIds(curEnv, rowIdStreamOfConstant(curEnv, column, constantValue, activeRowIds, comparator));
  }

  /**
   * Executes a terminal operation on the given stream that will send the row IDs to all output {@link RowIdConsumer}s.
   * 
   * @param rowIdStreamPair
   *          Pair of stream producing rowIds, and the {@link QueryUuidThreadState} that should be re-constructed as
   *          soon as the terminal operation on the stream was executed.
   */
  private void sendRowIds(ExecutionEnvironment curEnv, Pair<Stream<Long>, QueryUuidThreadState> rowIdStreamPair) {
    Stream<Long> rowIdStream = rowIdStreamPair.getLeft();
    QueryUuidThreadState uuidState = rowIdStreamPair.getRight();
    rowIdStream. //
        collect(new HashingBatchCollector<Long>( // RowIds are unique, so using BatchCollector is ok.
            100, // Batch size
            len -> new Long[len], // new result array
            new Consumer<Long[]>() { // Batch-collect the row IDs
              @Override
              public void accept(Long[] t) {
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
  }

  /**
   * Executes a comparison of the given column to the given constant value.
   * 
   * <p>
   * The left operand to the comparison is the column, the right one is the constant:
   * 
   * Example: COL >= constant
   * 
   * @return A Stream containing the RowIds that matched and the {@link QueryUuidThreadState} that should be
   *         re-constructed as soon as the terminal operation on the stream was executed.
   */
  private Pair<Stream<Long>, QueryUuidThreadState> rowIdStreamOfConstant(ExecutionEnvironment env,
      StandardColumnShard column, Object constantValue, NavigableSet<Long> activeRowIds, RowIdComparator comparator) {
    Long referenceColumnValueId = comparator.findReferenceColumnValueId(column, constantValue);

    QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();

    if (referenceColumnValueId == null)
      // no entry matches, return empty stream.
      return new Pair<>(new ArrayList<Long>().stream(), uuidState);

    return new Pair<>(column.getPages().values().stream().parallel(). // stream all Pages in parallel
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

              return comparator.pageContainsAnyRelevantValue(page, referenceColumnValueId);
            } finally {
              QueryUuid.clearCurrent();
            }
          }
        }).map(new Function<ColumnPage, Pair<ColumnPage, Set<Long>>>() { // find ColumnPageValue IDs that match the
                                                                         // comparison
          @Override
          public Pair<ColumnPage, Set<Long>> apply(ColumnPage page) {
            QueryUuid.setCurrentThreadState(uuidState);
            try {
              queryRegistry.getOrCreateCurrentStatsManager().registerPageAccess(page,
                  env.isTemporaryColumn(column.getName()));

              Set<Long> pageValueIds = comparator.findActivePageValueIds(page, referenceColumnValueId);
              return new Pair<>(page, pageValueIds);
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

                for (Long rowId : activeRowIdsInThisPage) {
                  // TODO #7 perhaps decompress whole value array, as it may be RLE encoded anyway.
                  Long decompressedColumnPageId = page.getValues().get((int) (rowId - page.getFirstRowId()));
                  if (searchedPageValueIds.contains(decompressedColumnPageId))
                    res.add(rowId);
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
   * Traverses the pages of two columns and finds rowIDs where the values of the two columns match the comparsion.
   * 
   * @param leftColumn
   *          The left column of the equation.
   * @param rightColumn
   *          The right column of the equation, the {@link QueryableColumnShard#getDelegate()} must return a
   *          {@link StandardColumnShard}.
   * @param activeRowIds
   *          Set of active row IDs, used to filter the column pages. Can be <code>null</code>.
   * @param comparator
   *          The comparator implementing >, >=, < or <=.
   */
  private void executeOnOtherCol(ExecutionEnvironment curEnv, StandardColumnShard leftColumn,
      QueryableColumnShard rightColumn, NavigableSet<Long> activeRowIds, RowIdComparator comparator) {

    NavigableMap<Long, Long> comparisonMap = comparator.calculateComparisonMap(leftColumn, rightColumn);

    Long[] colValueIds1 = comparisonMap.keySet().stream().sorted().toArray(l -> new Long[l]);

    QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();
    Stream<Long> resultRowIdStream;

    resultRowIdStream = leftColumn.getPages().values().stream().parallel().
        // filter out pairs that either do not match the rowID range or where the left page does not contain any
        // interesting value
    filter(new Predicate<ColumnPage>() {
      @Override
      public boolean test(ColumnPage leftColPage) {
        QueryUuid.setCurrentThreadState(uuidState);
        try {
          if (activeRowIds != null) {
            // If we're restricting the row IDs, we check if the page contains any row that we are interested in.
            Long interestedRowId = activeRowIds.ceiling(leftColPage.getFirstRowId());
            if (interestedRowId == null || interestedRowId > leftColPage.getFirstRowId() + leftColPage.size())
              return false;
          }

          if (!leftColPage.getColumnPageDict().containsAnyValue(colValueIds1))
            return false;

          return true;
        } finally {
          QueryUuid.clearCurrent();
        }
      }
    }).flatMap(new Function<ColumnPage, Stream<Long>>() {

      @Override
      public Stream<Long> apply(ColumnPage leftColPage) {
        QueryUuid.setCurrentThreadState(uuidState);

        try {
          // resolve ColumnPage value IDs from column value IDs for left page for all column value IDs we're
          // interested in.
          queryRegistry.getOrCreateCurrentStatsManager().registerPageAccess(leftColPage,
              curEnv.isTemporaryColumn(leftColumn.getName()));

          NavigableMap<Long, Long> leftPageIdsToColumnIds = new TreeMap<>();
          Long[] leftPageValueIds = leftColPage.getColumnPageDict().findIdsOfValues(colValueIds1);
          for (int i = 0; i < leftPageValueIds.length; i++)
            leftPageIdsToColumnIds.put(leftPageValueIds[i], colValueIds1[i]);

          List<Long> res = new ArrayList<>();

          // decompress value arrays and traverse them
          // TODO #2 STAT decide if full value array should be decompressed when there are activeRowIds.
          long[] leftValues = leftColPage.getValues().decompressedArray();

          for (int i = 0; i < leftValues.length; i++) {
            long rowId = leftColPage.getFirstRowId() + i;
            if (activeRowIds == null || activeRowIds.contains(rowId)) {
              long leftPageValueId = leftValues[i];
              Long leftColumnValueId = leftPageIdsToColumnIds.get(leftPageValueId);
              // check if we're interested in that column value ID.
              if (leftColumnValueId != null) {
                // TODO #2 STAT decide if we should decompress the whole array for the right side, too.
                if (comparator.rowMatches(leftColumnValueId, rowId, rightColumn, comparisonMap))
                  res.add(rowId);
              }
            }
          }
          return res.stream();
        } finally {
          QueryUuid.clearCurrent();
        }
      }
    });

    sendRowIds(curEnv, new Pair<>(resultRowIdStream, uuidState));

  }

  @Override
  public List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(
        Arrays.asList(new GenericConsumer[] { colBuiltConsumer, rowIdConsumer, columnVersionBuiltConsumer }));
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof RowIdConsumer) && !(consumer instanceof OverwritingRowIdConsumer))
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
    if (value != null)
      return "colName=" + colName + ",value=" + value;
    return "colName=" + colName + ",otherColName=" + otherColName;
  }

  /**
   * Implements one of the inequality comparisons supported by {@link RowIdInequalStep}.
   */
  public static interface RowIdComparator {
    /**
     * Finds a reference column value ID for a specific constant value.
     * 
     * <p>
     * The returned reference column value will later be used to call
     * {@link #pageContainsAnyRelevantValue(ColumnPage, Long)} and {@link #findActivePageValueIds(ColumnPage, Long)}.
     * 
     * <p>
     * This method is called if a column is compared to a constant value.
     * 
     * @return <code>null</code> in case the column does not contain /any/ element that matches the comparator.
     */
    public <T> Long findReferenceColumnValueId(ColumnShard column, Object value);

    /**
     * Quickly validates if a page contains any interesting rows when comparing to the given reference column value ID.
     * The latter was resolved before using {@link #findReferenceColumnValueId(ColumnShard, Object)}.
     * 
     * <p>
     * This method is called if a column is compared to a constant value.
     */
    public boolean pageContainsAnyRelevantValue(ColumnPage page, Long referenceValueColumnValueId);

    /**
     * Finds all rowIds that match the comparison of a column to a constant value, the latter being identified by its
     * column value ID which has been returned by a call to {@link #findReferenceColumnValueId(ColumnShard, Object)}
     * before.
     * 
     * <p>
     * This method is called if a column is compared to a constant value.
     */
    public Set<Long> findActivePageValueIds(ColumnPage page, Long referenceValueColumnValueId);

    /**
     * Calculates a comparison map used for a comparison between two columns.
     *
     * <p>
     * The returned map contains all interesting column value IDs of the leftCol as keys (= those column value IDs where
     * there are matching column value IDs of the rightCol). The value is typically a column value ID of the right col,
     * which though will be interpreted by {@link #rowMatches(long, long, ColumnShard, Map)} differently based on the
     * class implementing this interface.
     * 
     * <p>
     * This method is called if a column is compared to another column.
     * 
     * <p>
     * The two columns are expected to have the same column type.
     * 
     * @param rightCol
     *          although a {@link QueryableColumnShard}, this needs to be a {@link StandardColumnShard} (=
     *          {@link QueryableColumnShard#getDelegate()} needs to return a {@link StandardColumnShard}!)
     */
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        QueryableColumnShard rightCol);

    /**
     * Evaluates if a specific row where the leftCol matched a key the comparison map actually is a row that matches the
     * comparison and should therefore be returned by this step.
     * 
     * <p>
     * The comparison map used was created before using
     * {@link #calculateComparisonMap(StandardColumnShard, StandardColumnShard)}.
     * 
     * <p>
     * This method is called if a column is compared to another column.
     */
    public boolean rowMatches(long leftColumnValueId, long rowId, QueryableColumnShard rightCol,
        Map<Long, Long> comparisonMap);
  }

  /**
   * Implemented a 'greater or equal' comparison.
   */
  public static class GtEqRowIdComparator implements RowIdComparator {
    @Override
    public boolean pageContainsAnyRelevantValue(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().containsAnyValueGtEq(referenceValueColumnValueId);
    }

    @Override
    public Set<Long> findActivePageValueIds(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().findIdsOfValuesGtEq(referenceValueColumnValueId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Long findReferenceColumnValueId(ColumnShard column, Object value) {
      Long grEqId;

      try {
        grEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findGtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }
      if (grEqId == null)
        return null;

      // ignore positive/negative encoding
      if (grEqId < 0)
        grEqId = -(grEqId + 1);
      return grEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        QueryableColumnShard rightCol) {
      return ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findGtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, QueryableColumnShard rightCol,
        Map<Long, Long> comparisonMap) {
      long rightColumnValueId = rightCol.resolveColumnValueIdForRow(rowId);
      long comparisonOtherId = comparisonMap.get(leftColumnValueId);

      // ignore positive/negative encoding of findGrEqIds
      if (comparisonOtherId < 0)
        comparisonOtherId = -(comparisonOtherId + 1);

      return rightColumnValueId != -1 && rightColumnValueId <= comparisonOtherId;
    }

  }

  /**
   * Implements a string 'greater' comparison.
   */
  public static class GtRowIdComparator implements RowIdComparator {
    @Override
    public boolean pageContainsAnyRelevantValue(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().containsAnyValueGt(referenceValueColumnValueId);
    }

    @Override
    public Set<Long> findActivePageValueIds(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().findIdsOfValuesGt(referenceValueColumnValueId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Long findReferenceColumnValueId(ColumnShard column, Object value) {
      // search less than or equal ID, when doing a > search later, this will find us the right results.
      Long ltEqId;
      try {
        ltEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findLtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }

      if (ltEqId == null)
        // no value <= our searched value, therefore /all/ values are valid. As we compare with "gt" later, lets use -1
        // here.
        return -1L;

      // ignore positive/negative encoding
      if (ltEqId < 0)
        ltEqId = -(ltEqId + 1);

      return ltEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        QueryableColumnShard rightCol) {
      // start off with a 'greater or equal' map.
      NavigableMap<Long, Long> res = ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findGtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());

      for (Iterator<Entry<Long, Long>> it = res.entrySet().iterator(); it.hasNext();) {
        Entry<Long, Long> e = it.next();
        if (e.getValue() == 0) // leftCol == everything colB[i] for i <= 0 -> we're not interested in ==, therefore
                               // remove entry.
          it.remove();
        else if (e.getValue() > 0) // leftCol == colB[value]. As we want > relation, leftCol is > colB[value -1].
          e.setValue(e.getValue() - 1);
        else if (e.getValue() < 0) // leftCol is > colB[i] for i <= -(value + 1). See JavaDoc findGrEqIds.
          e.setValue(-(e.getValue() + 1));
      }

      return res;
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, QueryableColumnShard rightCol,
        Map<Long, Long> comparisonMap) {
      long rightColumnValueId = rightCol.resolveColumnValueIdForRow(rowId);
      return (rightColumnValueId != -1 && rightColumnValueId <= comparisonMap.get(leftColumnValueId));
    }
  }

  /**
   * Implements a 'less than or equal' comparison.
   */
  public static class LtEqRowIdComparator implements RowIdComparator {
    @Override
    public boolean pageContainsAnyRelevantValue(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().containsAnyValueLtEq(referenceValueColumnValueId);
    }

    @Override
    public Set<Long> findActivePageValueIds(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().findIdsOfValuesLtEq(referenceValueColumnValueId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Long findReferenceColumnValueId(ColumnShard column, Object value) {
      Long ltEq;
      try {
        ltEq = ((Dictionary<T>) column.getColumnShardDictionary()).findLtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }
      if (ltEq == null)
        return null;

      // ignore positive/negative encoding.
      if (ltEq < 0)
        ltEq = -(ltEq + 1);
      return ltEq;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        QueryableColumnShard rightCol) {
      return ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findLtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, QueryableColumnShard rightCol,
        Map<Long, Long> comparisonMap) {
      long rightColumnValueId = rightCol.resolveColumnValueIdForRow(rowId);
      long comparisonOtherID = comparisonMap.get(leftColumnValueId);

      // ignore positive/negative encoding of findLtEqIds
      if (comparisonOtherID < 0)
        comparisonOtherID = -(comparisonOtherID + 1);

      return rightColumnValueId != -1 && rightColumnValueId >= comparisonOtherID;
    }
  }

  /**
   * Implements a string 'less than' comparison.
   */
  public static class LtRowIdComparator implements RowIdComparator {
    @Override
    public boolean pageContainsAnyRelevantValue(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().containsAnyValueLt(referenceValueColumnValueId);
    }

    @Override
    public Set<Long> findActivePageValueIds(ColumnPage page, Long referenceValueColumnValueId) {
      return page.getColumnPageDict().findIdsOfValuesLt(referenceValueColumnValueId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Long findReferenceColumnValueId(ColumnShard column, Object value) {
      // find the ID of the next >= value compared to the requested one. When executing a strong < comparison later,
      // this will return the correct results.
      Long grEqId;
      try {
        grEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findGtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }
      if (grEqId == null) {
        Long maxId = column.getColumnShardDictionary().getMaxId();
        if (maxId == null)
          // shard dict is empty!
          return null;

        // return maxId + 1, as we compare using Lt later. That will give the correct results.
        return maxId + 1;
      }

      // ignore positive/negative result encoding.
      if (grEqId < 0)
        grEqId = -(grEqId + 1);

      return grEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        QueryableColumnShard rightCol) {
      // start off with a 'less than or equal' comparison map
      NavigableMap<Long, Long> res = ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findLtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());

      StandardColumnShard rightStandardCol = (StandardColumnShard) rightCol.getDelegate();

      for (Iterator<Entry<Long, Long>> it = res.entrySet().iterator(); it.hasNext();) {
        Entry<Long, Long> e = it.next();
        if (e.getValue() == rightCol.getFirstRowId() + rightStandardCol.getNumberOfRowsInColumnShard() - 1)
          // leftCol == everything colB[lastIdx] -> we're not interested in ==, therefore remove entry.
          it.remove();
        else if (e.getValue() > 0) // leftCol == colB[value]. As we want < relation, leftCol is < colB[value +1].
          e.setValue(e.getValue() + 1);
        else if (e.getValue() < 0) // leftCol is < colB[i] for i >= -(value + 1). See JavaDoc findLtEqIds.
          e.setValue(-(e.getValue() + 1));
      }

      return res;
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, QueryableColumnShard rightCol,
        Map<Long, Long> comparisonMap) {
      long rightColumnValueId = rightCol.resolveColumnValueIdForRow(rowId);
      return (rightColumnValueId != -1 && rightColumnValueId >= comparisonMap.get(leftColumnValueId));
    }
  }
}
