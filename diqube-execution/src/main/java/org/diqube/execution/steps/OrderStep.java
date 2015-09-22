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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.ColumnVersionBuiltHelper;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OrderedRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.ArrayViewLongList;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Orders the incoming row IDs by the values of potentially multiple columns, ascending or descending.
 * 
 * <p>
 * Any order step can have a LIMIT set which will cut off the results after the amount of rows. If a limit is specified,
 * a limitStart may be specified, denoting the first index of the row IDs to return. If <b>both</b> these values are
 * unset, <i>softLimit</i> may be set. A soft limit is needed for cluster nodes which might be able to order only by a
 * subset of the columns that actually need to be ordered according to the users request. That might happen if the ORDER
 * BY contained a column which is based on a group aggregation function - as remotes do not have the final grouped
 * aggregation values available, they obviously cannot order by them. We nevertheless want to faciliate using limits as
 * well as possible also on cluster nodes already to reduce the data transfers to the query master and the memory
 * consumption on the query master. Therefore there is a soft limit, which will cut off any results, too, following
 * these requirements:
 * 
 * <ul>
 * <li>If softLimit is set, this step will try to return only softLimit number of rows, just like when LIMIT is set.
 * <li>If, though, there are any rows after sorting which would be cut off, but whose values of the ordered columns is
 * equal to any of the rows that are not cut off, then these rows will be contained in the result, too. This then
 * enables these "equal" rows to be sorted further by the query master and then being cut off correctly.
 * <li>As the rowIDs are ordered already before executing the cut-off, this step only inspects the last row that is not
 * cut-off and compares that to any rows marked for cut-off - if there are equal rows, they will be included.
 * </ul>
 * 
 * <p>
 * When this step is executed on the query master, there will usually be (at least) one
 * {@link ColumnVersionBuiltConsumer} wired. This means that the ordering will take place on the intermediary column
 * values of a {@link VersionedExecutionEnvironment}. In that case, the actual cut-off by any limits is not executed
 * while working on those intermediary column, but only when the columns have been built fully. This is because each
 * value in each of the interesting columns might change arbitrarily in a intermediary column and we therefore cannot
 * guarantee that a rowId that looks like not being cut-off in the first run, might get values that would force us to
 * not cut-off that row ID in a later execution.
 *
 * <p>
 * The columns that are ordered by are expected to be {@link StandardColumnShard}s.
 *
 * <p>
 * Input: 1 {@link RowIdConsumer}, optionally multiple {@link ColumnBuiltConsumer}s, optionally multiple
 * {@link ColumnVersionBuiltConsumer}s <br>
 * Output: {@link RowIdConsumer} and/or {@link OrderedRowIdConsumer}
 *
 * @author Bastian Gloeckle
 */
public class OrderStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(OrderStep.class);

  private AtomicBoolean columnBuiltInputIsDone = new AtomicBoolean(false);
  /**
   * Interesting only, if ColumnBuiltConsumer is wired. Then it contains the names of the columns we still wait for to
   * be fully built.
   */
  private Set<String> columnsThatNeedToBeBuilt;
  /**
   * <code>true</code> when all columns this step is waiting for have been built and are available in
   * {@link #defaultEnv} .
   */
  private AtomicBoolean allColumnsBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer columnBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void doColumnBuilt(String colName) {
      columnsThatNeedToBeBuilt.remove(colName);

      if (columnsThatNeedToBeBuilt.isEmpty())
        allColumnsBuilt.set(true);
    }

    @Override
    protected void allSourcesAreDone() {
      columnBuiltInputIsDone.set(true);
    }
  };

  /**
   * The newest available {@link VersionedExecutionEnvironment} which should be used to query values of columns. This
   * will be set only if a {@link ColumnVersionBuiltConsumer} is wired.
   */
  private VersionedExecutionEnvironment newestTemporaryEnv = null;

  private AbstractThreadedColumnVersionBuiltConsumer columnVersionBuiltConsumer =
      new AbstractThreadedColumnVersionBuiltConsumer(this) {
        @Override
        protected void allSourcesAreDone() {
          // we rely on ColumnBuiltConsumer to report the final build.
        }

        @Override
        protected synchronized void doColumnBuilt(VersionedExecutionEnvironment env, String colName,
            Set<Long> adjustedRowIds) {
          if (newestTemporaryEnv == null)
            newestTemporaryEnv = env;
          else if (newestTemporaryEnv.getVersion() < env.getVersion())
            newestTemporaryEnv = env;
        }
      };

  private AtomicBoolean rowIdSourceIsEmpty = new AtomicBoolean(false);
  /** input rowIDs as reported by input {@link RowIdConsumer}. */
  private ConcurrentLinkedDeque<Long> rowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      OrderStep.this.rowIdSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        OrderStep.this.rowIds.add(rowId);
    }
  };
  /**
   * current version of sorted row IDs. This array may be longer than how many values are actually available. See
   * {@link #sortedRowIdsLength}.
   */
  private Long[] sortedRowIds = new Long[0];
  /** Number of valid entries in {@link #sortedRowIds}. See {@link #resizeArrayForLength(Long[], int)}. */
  private int sortedRowIdsLength = 0;
  /**
   * maximum value of {@link #sortedRowIdsLength} that we should consider when taking into account the LIMIT and
   * {@link #limitStart}. Only valid of {@link #softLimit} is <code>null</code>!
   */
  private Long sortedRowIdsMaxLength;
  private Long limitStart;
  private Long softLimit;

  /**
   * Creates a {@link SortComparator} that sorts according to the requested parameters. This can be called as soon as
   * all columns that this {@link OrderStep} is interested have been created (= {@link #columnBuiltConsumer} in the
   * given {@link ExecutionEnvironment}
   */
  private Function<ExecutionEnvironment, SortComparator> headComparatorProvider;
  /**
   * List of columns to sort by, just like the user specified it in the query. Pair is col Name to boolean (true if ASC,
   * false if DESC).
   */
  private List<Pair<String, Boolean>> sortCols;
  /** A Set with just the names of the columns to sort by (unordered). */
  private Set<String> sortColSet;

  private ExecutionEnvironment defaultEnv;

  /**
   * row IDs that were reported as active by the input {@link RowIdConsumer}, but which have not yet been inspected
   * because there are no values available for all columns at these rows. This is only interesting if there are
   * {@link ColumnVersionBuiltConsumer}s wired (= only on query master).
   */
  private NavigableSet<Long> notYetProcessedRowIds = new TreeSet<>();

  /**
   * 
   * @param sortCols
   *          List of Pairs of which columns should be sorted after (in that order!). Left side of the pair is the
   *          column name, right side is <code>true</code> if sorted should be ascending, <code>false</code> if
   *          descending.
   * @param limit
   *          if not <code>null</code>, a limit on the resulting set of row IDs will be applied, that means at most that
   *          many rows will be returned (taking into account the ordering, of course).
   * @param limitStart
   *          if not <code>null</code>, not the _first_ (limit) row IDs will be returned, but the ones starting at index
   *          limitStart. This cannot be set, if limit is <code>null</code>.
   * @param softLimit
   *          If both limit and limitStart are null, this field might be set, otherwise it needs to be <code>null</code>
   *          . For a description, see class comment.
   */
  public OrderStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      List<Pair<String, Boolean>> sortCols, Long limit, Long limitStart, Long softLimit) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.sortCols = sortCols;
    this.limitStart = limitStart;
    this.softLimit = softLimit;
    if (limit != null) {
      sortedRowIdsMaxLength = limit;
      if (limitStart != null)
        sortedRowIdsMaxLength += limitStart;
    }
  }

  @Override
  public void initialize() {
    sortColSet = sortCols.stream().map(p -> p.getLeft()).collect(Collectors.toSet());
    columnsThatNeedToBeBuilt = new ConcurrentSkipListSet<>(sortColSet);
    for (Iterator<String> it = columnsThatNeedToBeBuilt.iterator(); it.hasNext();)
      if (defaultEnv.getColumnShard(it.next()) != null)
        it.remove();

    // factory method for comparators based on a specific env.
    headComparatorProvider = (executionEnvironment) -> {
      SortComparator headComparator = null;
      SortComparator lastComparator = null;
      for (Pair<String, Boolean> sortCol : sortCols) {
        String colName = sortCol.getLeft();
        boolean sortAsc = sortCol.getRight();

        QueryableColumnShard column = executionEnvironment.getColumnShard(colName);
        ColumnValueIdResolver resolver = (rowId) -> column.resolveColumnValueIdForRow(rowId);

        SortComparator newComparator = new SortComparator(resolver, sortAsc);
        if (lastComparator != null)
          lastComparator.setDelegateComparatorOnEqual(newComparator);
        else
          headComparator = newComparator;
        lastComparator = newComparator;
      }
      return headComparator;
    };
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof OrderedRowIdConsumer)
        && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only OrderedRowIdConsumer and RowIdConsumer accepted.");
  }

  @Override
  protected void execute() {
    // intermediateRun = true if NOT all final versions of all columns have been built and are available in defaultEnv
    // -> we only have intermediary values!
    boolean intermediateRun = !(columnBuiltConsumer.getNumberOfTimesWired() == 0 || allColumnsBuilt.get());

    if (columnBuiltConsumer.getNumberOfTimesWired() > 0 && columnBuiltInputIsDone.get() && !allColumnsBuilt.get()) {
      logger.debug("Ordering needs to wait for a column to be built, but it won't be built. Skipping.");
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
      return;
    }

    ExecutionEnvironment env;
    if (!intermediateRun) {
      env = defaultEnv;
    } else {
      env = newestTemporaryEnv;
      if (env == null)
        return;
      // we'll be sorting on a version of the Env that contains only intermediate columns. Check if we have at least
      // some values for all interesting columns.
      boolean allColsAvailable = sortColSet.stream().allMatch(colName -> env.getColumnShard(colName) != null);
      if (!allColsAvailable)
        return;
    }

    // new row IDs we ought to order into the result.
    NavigableSet<Long> activeRowIdsSet = new TreeSet<>();
    Long tmpNextRowId;
    while ((tmpNextRowId = rowIds.poll()) != null)
      activeRowIdsSet.add(tmpNextRowId);

    SortComparator headComparator = headComparatorProvider.apply(env);

    if (intermediateRun) {
      // Make sure that we only order those rows, where we have values for all columns.
      // Please note the following:
      // We only make sure that each column contains /any/ value on the row IDs, these might be as well default values
      // filled in by SparseColumnShardBuilder! We therefore might order based on "wrong" values here. But this is not
      // as important, because as soon as we have correct values and the orderStep is executed again (either still in
      // 'intermediary' mode or in 'isLastRun' mode) the row will be ordered correctly.
      new ColumnVersionBuiltHelper().publishActiveRowIds(env, sortColSet, activeRowIdsSet, notYetProcessedRowIds);
    } else {
      if (notYetProcessedRowIds.size() > 0) {
        activeRowIdsSet.addAll(notYetProcessedRowIds);
        notYetProcessedRowIds.clear();
      }
    }

    logger.trace(
        "Starting to order based on Env {}, having active RowIDs (limt) {}, not yet processed (limit) {}. intermediateRun: {}",
        env, Iterables.limit(activeRowIdsSet, 100), Iterables.limit(notYetProcessedRowIds, 100), intermediateRun);

    if (activeRowIdsSet.size() > 0) {

      Long[] activeRowIds = activeRowIdsSet.toArray(new Long[activeRowIdsSet.size()]);

      // be sure that we have enough space in the array
      sortedRowIds = resizeArrayForLength(sortedRowIds, sortedRowIdsLength + activeRowIds.length);

      // add new values to the array and use insertion sort to put them at the right sorted locations
      System.arraycopy(activeRowIds, 0, sortedRowIds, sortedRowIdsLength, activeRowIds.length);
      sortedRowIdsLength += activeRowIds.length;
    }

    // Use default JVM sorting, which implements a TimSort at least in OpenJDK - this executes very well even on
    // partly sorted arrays (which is [mostly] true in our case if the execute method is executed at least twice).
    // -
    // Execute this in each run. This is needed, as either there were new rowIds added or some rows changed their values
    // (otherwise execute() would not be called). Therefore we always need to sort the array.
    // TODO #8 support sorting only /some/ elements in case we're based on intermediary values.
    Arrays.sort(sortedRowIds, 0, sortedRowIdsLength, headComparator);

    // cutOffPoint = first index in sortedRowIds that would be cut off by a limit/softLimit clause. null otherwise.
    Integer cutOffPoint = null;

    // Find cut off point according to limit/softLimit.
    if (softLimit == null) {
      if (sortedRowIdsMaxLength != null && sortedRowIdsLength > sortedRowIdsMaxLength) {
        // we have a LIMIT set but our sorted array is already longer. Cut its length to save some memory.

        // remember point to cut off at
        cutOffPoint = sortedRowIdsMaxLength.intValue();
      }
    } else {
      if (sortedRowIdsLength > softLimit) {
        // we are above the soft limit!
        Long lastContainedRowId = sortedRowIds[softLimit.intValue() - 1];
        int softLength = softLimit.intValue();
        // linearily search those rows which should be included in the result although they would have been cut-off.
        while (softLength < sortedRowIdsLength
            && headComparator.compare(lastContainedRowId, sortedRowIds[softLength]) == 0)
          softLength++;

        logger.trace("Hit soft limit {}, using length {}", softLimit.intValue(), softLength);

        // remember point to cut off at
        cutOffPoint = softLength;
      }
    }

    // inform RowIdConsumers about new row IDs
    if (activeRowIdsSet.size() > 0) {
      if (intermediateRun) {
        // if this is an intermediary run, make sure that we report all rowIds to subsequent steps, as we did not
        // actually execute the cut-off.
        Long[] rowIds = activeRowIdsSet.stream().toArray(l -> new Long[l]);
        forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(rowIds));
      } else {
        // This is a last run. This means that sorting is now based on the defaultEnv and that none of the columns might
        // change its values anymore.
        // If there were new rowIds, we need to report only those that actually are included inside
        // the result value (and are not cut-off).
        Long[] rowIdsToBeOutput;

        if (cutOffPoint != null)
          rowIdsToBeOutput = findRowIdsToBeOutputOnCutOff(activeRowIdsSet, cutOffPoint);
        else
          rowIdsToBeOutput = activeRowIdsSet.stream().toArray(l -> new Long[l]);

        // report all row IDs so subsequent RowID consumers can handle them. We might report too much rowIDs here.
        forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(rowIdsToBeOutput));
      }
    }

    if (!intermediateRun && cutOffPoint != null)
      // execute cut off if we're not in an intermediate run. While in an intermediate run we're based on arbitrary
      // versions of the column (see ColumnVersionBuiltConsumer). This means that the values of all interesting rows
      // in all columns might change arbitrarily. Therefore we cannot execute a cut-off in that case, as a row that
      // we'd cut off now might change its value later on so we'd actually need to include it. On the other hand, not
      // only that row might change its value, but all other rows might change their values and force that row to be
      // inside the result set - but if we cut it off before, there's no chance to recover it. So we execute no
      // cut-off in that case.
      sortedRowIdsLength = cutOffPoint;

    logger.trace("Ordering result (limit): {}",
        Iterables.limit(Arrays.asList(sortedRowIds), Math.min(20, sortedRowIdsLength)));

    final Integer finalIntendedCutOffPoint = cutOffPoint;

    // output sorted result in each run - it might be that we did not have new row IDs, but the value of specific row
    // IDs were changed and therefore we have a now ordering (after sorting above) - we should publicize that.
    forEachOutputConsumerOfType(OrderedRowIdConsumer.class, new Consumer<OrderedRowIdConsumer>() {
      @Override
      public void accept(OrderedRowIdConsumer orderedConsumer) {
        int startIdx;
        int length;
        if (limitStart != null) {
          startIdx = limitStart.intValue();
          length = sortedRowIdsLength - limitStart.intValue();
        } else {
          startIdx = 0;
          length = sortedRowIdsLength;
        }

        if (intermediateRun && finalIntendedCutOffPoint != null) {
          // we did not execute a cut-off, as we're in an intermediary run. So do at least a 'virtual cut-off' here, by
          // only reporting those rows inside the intendedCutOffPoint to the steps interested in the ordering.
          length -= sortedRowIdsLength - finalIntendedCutOffPoint;
        }

        orderedConsumer.consumeOrderedRowIds(new ArrayViewLongList(sortedRowIds, startIdx, length));
      }
    });

    // check if we're done.
    if (!intermediateRun && rowIdSourceIsEmpty.get() && rowIds.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  /**
   * Find those rowIDs that were in activeRowIdsSet, but which will be cut-off when using the given cut-off-point.
   * 
   * <p>
   * This needs to be executed before executing a cutOff (=adjusting {@link #sortedRowIdsLength}), as this method needs
   * that value.
   * 
   * @param activeRowIdsSet
   *          The source set of row IDs.
   * @param cutOffPoint
   *          The first index in {@link #sortedRowIds} that will be cut-off.
   * @return the row IDs to be reported as newly added. These will be all the longs in activeRowIdsSet which are NOT cut
   *         off.
   */
  private Long[] findRowIdsToBeOutputOnCutOff(Set<Long> activeRowIdsSet, int cutOffPoint) {
    Set<Long> rowIdsBeingCutOff = new HashSet<>();
    for (int i = cutOffPoint; i < sortedRowIdsLength; i++)
      rowIdsBeingCutOff.add(sortedRowIds[i]);
    logger.trace("Cutting off {} results because of (soft) limit clause: (limt) {}", sortedRowIdsLength - cutOffPoint,
        Iterables.limit(rowIdsBeingCutOff, 100));

    return Sets.difference(activeRowIdsSet, rowIdsBeingCutOff).stream().toArray(l -> new Long[l]);
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer, columnBuiltConsumer, columnVersionBuiltConsumer });
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // there may be an arbitrary number of ColumnBuiltConsumers, but there has to be at least the RowIdConsumer
    if (rowIdConsumer.getNumberOfTimesWired() == 0)
      throw new ExecutablePlanBuildException("RowIdConsumer is not wired.");
  }

  private Long[] resizeArrayForLength(Long[] longArray, int requestedNewLength) {
    if (longArray != null && requestedNewLength <= longArray.length)
      return longArray;

    int newLength;
    int highestBit = Integer.highestOneBit(requestedNewLength);
    if (highestBit == 0)
      highestBit = 1;
    if ((highestBit << 1) > requestedNewLength)
      newLength = highestBit << 1;
    else
      newLength = Integer.MAX_VALUE;

    Long[] resultArray = new Long[newLength];
    if (longArray != null)
      System.arraycopy(longArray, 0, resultArray, 0, longArray.length);

    return resultArray;
  }

  /**
   * Resolves a row ID to a Column value ID.
   */
  private interface ColumnValueIdResolver {
    public long resolveColumnValueId(long rowId);
  }

  /**
   * A {@link Comparator} that compared the column value IDs of the rowIDs that are expected to be provided to the
   * {@link #compare(Long, Long)} method. If the column value IDs are equal, the comparator may forward the decision to
   * another {@link SortComparator} - this enables us to implement a detailed sorting using multiple columns after each
   * other.
   */
  private class SortComparator implements Comparator<Long> {

    private ColumnValueIdResolver columnValueIdResolver;
    private SortComparator delegateComparatorOnEqual = null;
    private int orderFactor;

    public SortComparator(ColumnValueIdResolver columnValueIdResolver, boolean sortAscending) {
      this.columnValueIdResolver = columnValueIdResolver;
      orderFactor = (sortAscending) ? 1 : -1;
    }

    @Override
    public int compare(Long rowId1, Long rowId2) {
      long colValue1 = columnValueIdResolver.resolveColumnValueId(rowId1);
      long colValue2 = columnValueIdResolver.resolveColumnValueId(rowId2);
      if (colValue1 == colValue2) {
        if (delegateComparatorOnEqual != null)
          return delegateComparatorOnEqual.compare(rowId1, rowId2);
        return 0;
      }
      return orderFactor * Long.compare(colValue1, colValue2);
    }

    public void setDelegateComparatorOnEqual(SortComparator delegateComparatorOnEqual) {
      this.delegateComparatorOnEqual = delegateComparatorOnEqual;
    }
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "sortCols=" + sortCols;
  }
}
