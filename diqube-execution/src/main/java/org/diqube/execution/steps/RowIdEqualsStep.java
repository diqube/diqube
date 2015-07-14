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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.diqube.data.Dictionary;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.dict.DoubleDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.data.str.dict.StringDictionary;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
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
 * {@link ColumnPageConsumer} input needs to be specified which keeps this step up to date with the construction of that
 * column. If no {@link ColumnPageConsumer} is specified, then simply the full column is searched.
 * 
 * <p>
 * Additionally, this step can be wired to the output of another {@link RowIdConsumer} which will force this instance to
 * only take those RowIds into account that are provided by the input {@link RowIdConsumer} - effectivley building a AND
 * concatenation. In contrast to a {@link RowIdAndStep} though, the two {@link RowIdEqualsStep}s that are connected that
 * way would be executed after each other, not parallel to each other. Therefore, usually a {@link RowIdAndStep} is
 * used.
 * 
 * <p>
 * Only {@link StandardColumnShard} supported.
 * 
 * <p>
 * Input: 1 optional {@link ColumnPageConsumer}, 1 optional {@link RowIdConsumer} <br>
 * Output: {@link RowIdConsumer}s.
 *
 * @author Bastian Gloeckle
 */
public class RowIdEqualsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(RowIdEqualsStep.class);

  /** Only important if {@link #colBuiltConsumerIsWired} == true */
  private AtomicBoolean columnIsBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {

    @Override
    protected void doColumnBuilt(String colName) {
    }

    @Override
    protected void allSourcesAreDone() {
      RowIdEqualsStep.this.columnIsBuilt.set(true);
    }
  };

  private AtomicBoolean rowIdSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> inputRowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdEqualsStep.this.rowIdSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdEqualsStep.this.inputRowIds.add(rowId);
    }
  };

  private ExecutionEnvironment env;
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
   * @param sortedValues
   *          Expected to be sorted!
   */
  public RowIdEqualsStep(int stepId, ExecutionEnvironment env, String colName, Object[] sortedValues) {
    super(stepId);
    this.env = env;
    this.colName = colName;
    this.values = sortedValues;
    this.otherColName = null;
  }

  /**
   */
  public RowIdEqualsStep(int stepId, ExecutionEnvironment env, String colName, String otherColName) {
    super(stepId);
    this.env = env;
    this.colName = colName;
    this.otherColName = otherColName;
    this.values = null;
  }

  @Override
  public void execute() {
    if (colBuiltConsumer.getNumberOfTimesWired() > 0 && !columnIsBuilt.get())
      // we need to wait for a column to be built but it is not yet built.
      return;

    NavigableSet<Long> activeRowIds = null;
    if (rowIdConsumer.getNumberOfTimesWired() > 0) {
      activeRowIds = new TreeSet<>();
      Long rowId;
      while ((rowId = inputRowIds.poll()) != null)
        activeRowIds.add(rowId);

      if (activeRowIds.isEmpty()) {
        if (rowIdSourceIsEmpty.get() && inputRowIds.isEmpty()) {
          forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
          doneProcessing();
          return;
        }
      }
    }

    if (env.getColumnShard(colName) == null)
      throw new ExecutablePlanExecutionException("Could not find column " + colName);

    Dictionary<?> columnShardDictionary = env.getColumnShard(colName).getColumnShardDictionary();
    Collection<ColumnPage> pages = ((StandardColumnShard) env.getColumnShard(colName)).getPages().values();

    if (pages.size() > 0) {
      if (values != null) {
        // we're supposed to compare one column to constant values.

        if (columnValueIdsOfSearchedValues == null) {
          switch (env.getColumnType(colName)) {
          case LONG:
            if (!(values instanceof Long[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-number values.");
            columnValueIdsOfSearchedValues = ((LongDictionary) columnShardDictionary).findIdsOfValues((Long[]) values);
            break;
          case STRING:
            if (!(values instanceof String[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-string values.");
            columnValueIdsOfSearchedValues =
                ((StringDictionary) columnShardDictionary).findIdsOfValues((String[]) values);
            break;
          case DOUBLE:
            if (!(values instanceof Double[]))
              throw new ExecutablePlanExecutionException(
                  "Cannot compare column " + colName + " with non-floating point values.");
            columnValueIdsOfSearchedValues =
                ((DoubleDictionary) columnShardDictionary).findIdsOfValues((Double[]) values);
            break;
          }
        }

        rowIdEqualsConstants(columnValueIdsOfSearchedValues, pages, activeRowIds);
      } else {
        // we're supposed to compare to cols to each other.
        if (env.getColumnShard(otherColName) == null)
          throw new ExecutablePlanExecutionException("Could not find column " + otherColName);

        if (!env.getColumnType(colName).equals(env.getColumnType(otherColName)))
          throw new ExecutablePlanExecutionException("Cannot compare column " + colName + " to column " + otherColName
              + " as they have different data types.");

        Collection<ColumnPage> pages2 = ((StandardColumnShard) env.getColumnShard(otherColName)).getPages().values();

        if (pages.size() > 0 && pages2.size() > 0) {
          NavigableMap<Long, Long> equalColumnValueIds =
              findEqualIds(env.getColumnShard(colName), env.getColumnShard(otherColName));

          rowIdEqualsOtherColLong(pages, pages2, equalColumnValueIds, activeRowIds);
        }
      }
    }

    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
    doneProcessing();
  }

  @SuppressWarnings("unchecked")
  private <T> NavigableMap<Long, Long> findEqualIds(ColumnShard shard1, ColumnShard shard2) {
    return ((Dictionary<T>) shard1.getColumnShardDictionary())
        .findEqualIds((Dictionary<T>) shard2.getColumnShardDictionary());
  }

  private void rowIdEqualsConstants(Long[] columnValueIdsOfSearchedValues, Collection<ColumnPage> pages,
      NavigableSet<Long> activeRowIds) {
    sendRowIds(rowIdEqualsStream(columnValueIdsOfSearchedValues, pages, activeRowIds));
  }

  /**
   * Executes a terminal operation on the given stream that will send the row IDs to all output {@link RowIdConsumer}s.
   */
  private void sendRowIds(Stream<Long> rowIdStream) {
    rowIdStream. //
        collect(new HashingBatchCollector<Long>( // RowIds are unique, so using BatchCollector is ok.
            100, // Batch size
            len -> new Long[len], // new result array
            new Consumer<Long[]>() { // Batch-collect the row IDs
              @Override
              public void accept(Long[] t) {
                forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(t));
              }
            }));
  }

  /**
   * Searches row IDs containing specific values.
   * 
   * @param columnValueIdsOfSearchedValues
   *          The ColumnValueIDs of the searched values.
   * @param pages
   *          All pages of the column that should be inspected
   * @param activeRowIds
   *          The row IDs that are 'active' - only these row IDs will be searched for the given column value IDs. These
   *          row IDs will be used in order to reduce the number of pages that will be inspected. May be
   *          <code>null</code>.
   * @return Stream of rowIDs where the column is equal to one of the given values. If activeRowIds is set, this
   *         returned stream contains a (non-strict) sub-set of those activeRowIds.
   */
  private Stream<Long> rowIdEqualsStream(Long[] columnValueIdsOfSearchedValues, Collection<ColumnPage> pages,
      NavigableSet<Long> activeRowIds) {
    return pages.stream().parallel(). // stream all Pages in parallel
        filter(new Predicate<ColumnPage>() { // filter out inactive Pages
          @Override
          public boolean test(ColumnPage page) {
            if (activeRowIds != null) {
              // If we're restricting the row IDs, we check if the page contains any row that we are interested in.
              Long interestedRowId = activeRowIds.ceiling(page.getFirstRowId());
              if (interestedRowId == null || interestedRowId > page.getFirstRowId() + page.size())
                return false;
            }

            return page.getColumnPageDict().containsAnyValue(columnValueIdsOfSearchedValues);
          }
        }).map(new Function<ColumnPage, Pair<ColumnPage, Set<Long>>>() { // find ColumnPageValue IDs for active
                                                                         // Pages
          @Override
          public Pair<ColumnPage, Set<Long>> apply(ColumnPage page) {
            Long[] pageValueIdsArray = page.getColumnPageDict().findIdsOfValues(columnValueIdsOfSearchedValues);
            Set<Long> pageValueIdsSet = new HashSet<Long>(Arrays.asList(pageValueIdsArray));
            return new Pair<>(page, pageValueIdsSet);
          }
        }).flatMap(new Function<Pair<ColumnPage, Set<Long>>, Stream<Long>>() { // resolve RowIDs and map them flat
          // into a single stream
          @Override
          public Stream<Long> apply(Pair<ColumnPage, Set<Long>> pagePair) {
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
          }
        });
  }

  /**
   * Finds row IDs where two columns have the same value.
   * 
   * @param pages1
   *          All {@link ColumnPage}s of the first column.
   * @param pages2
   *          All {@link ColumnPage}s of the second column.
   * @param equalColumnValueIds
   *          the result of e.g. {@link LongDictionary#findEqualIds(LongDictionary)} - keys are column value ids for
   *          pages1 mapped to the column value ids for pages2 that represent equal values.
   * @param activeRowIds
   *          If there are any, the set of row IDs that should be inspected at most.
   */
  private void rowIdEqualsOtherColLong(Collection<ColumnPage> pages1, Collection<ColumnPage> pages2,
      NavigableMap<Long, Long> equalColumnValueIds, NavigableSet<Long> activeRowIds) {
    Long[] columnValueIds1 = equalColumnValueIds.keySet().stream().toArray((l) -> new Long[l]);
    Long[] columnValueIds2 = equalColumnValueIds.values().stream().sorted().toArray((l) -> new Long[l]);

    // TODO #2 STAT use statistics to first search the column that produces less results.

    // first: Find rows in col1 that have the given value
    Stream<Long> stillActiveRowIds = rowIdEqualsStream(columnValueIds1, pages1, activeRowIds);

    // then: search in the resulting Row ID stream those rowIds that have a valid value in col2.
    Stream<Long> equalRowIds =
        rowIdEqualsStream(columnValueIds2, pages2, stillActiveRowIds.collect(DiqubeCollectors.toNavigableSet()));

    sendRowIds(equalRowIds);
  }

  @Override
  public List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { colBuiltConsumer, rowIdConsumer }));
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only RowId consumers accepted!");
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // noop. If the input is wired it's fine, but if it isn't, it's fine, too.
  }

  @Override
  protected String getAdditionalToStringDetails() {
    if (values != null)
      return "colName=" + colName + ",values=" + Arrays.toString(values);
    return "colName=" + colName + ",otherColName=" + otherColName;
  }
}
