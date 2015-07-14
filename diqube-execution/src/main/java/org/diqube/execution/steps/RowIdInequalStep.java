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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.diqube.data.Dictionary;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
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
 * <p> Only {@link StandardColumnShard}s supported.
 * 
 * <p> Input: 1 optional {@link ColumnPageConsumer}, 1 optional {@link RowIdConsumer} <br> Output: {@link RowIdConsumer}
 * s.
 *
 * @author Bastian Gloeckle
 */
public class RowIdInequalStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(RowIdInequalStep.class);

  /** Only important if {@link #colBuiltConsumerIsWired} == true */
  private AtomicBoolean columnIsBuilt = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {

    @Override
    protected void doColumnBuilt(String colName) {
    }

    @Override
    protected void allSourcesAreDone() {
      RowIdInequalStep.this.columnIsBuilt.set(true);
    }
  };

  private AtomicBoolean rowIdSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> inputRowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdInequalStep.this.rowIdSourceIsEmpty.set(true);
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
   * The left operand to the comparison will always be the column, the right operand the constant.
   * 
   * @param value
   *          The constant to compare to.
   * @param comparator
   *          Freshly created instance of an implementation of {@link RowIdComparator}. If this step should compare with
   *          > {@link GtRowIdComparator}, if >= then {@link GtEqRowIdComparator}, if < {@link LtRowIdComparator}, if <=
   *          {@link LtEqRowIdComparator}.
   */
  public RowIdInequalStep(int stepId, ExecutionEnvironment defaultEnv, String colName, Object value,
      RowIdComparator comparator) {
    super(stepId);
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
   */
  public RowIdInequalStep(int stepId, ExecutionEnvironment env, String colName, String otherColName,
      RowIdComparator comparator) {
    super(stepId);
    this.defaultEnv = env;
    this.colName = colName;
    this.otherColName = otherColName;
    this.comparator = comparator;
    this.value = null;
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

    if (defaultEnv.getColumnShard(colName) == null)
      throw new ExecutablePlanExecutionException("Could not find column " + colName);

    StandardColumnShard columnShard = (StandardColumnShard) defaultEnv.getColumnShard(colName);
    NavigableMap<Long, ColumnPage> pages = columnShard.getPages();

    if (pages.size() > 0) {
      if (value != null) {
        // we're supposed to compare one column to constant values.

        compareToConstant(value, columnShard, activeRowIds, comparator);
      } else {
        // we're supposed to compare to cols to each other.
        if (defaultEnv.getColumnShard(otherColName) == null)
          throw new ExecutablePlanExecutionException("Could not find column " + otherColName);

        if (!defaultEnv.getColumnType(colName).equals(defaultEnv.getColumnType(otherColName)))
          throw new ExecutablePlanExecutionException("Cannot compare column " + colName + " to column " + otherColName
              + " as they have different data types.");

        StandardColumnShard otherColumnShard = (StandardColumnShard) defaultEnv.getColumnShard(otherColName);

        if (otherColumnShard.getPages().size() > 0)
          executeOnOtherCol(columnShard, otherColumnShard, activeRowIds, comparator);
      }
    }

    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
    doneProcessing();
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
  private void compareToConstant(Object constantValue, StandardColumnShard column, NavigableSet<Long> activeRowIds,
      RowIdComparator comparator) {
    sendRowIds(rowIdStreamOfConstant(column, constantValue, activeRowIds, comparator));
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
   * Executes a comparison of the given column to the given constant value.
   * 
   * <p>
   * The left operand to the comparison is the column, the right one is the constant:
   * 
   * Example: COL >= constant
   * 
   * @return A Stream containing the RowIds that matched.
   */
  private Stream<Long> rowIdStreamOfConstant(StandardColumnShard column, Object constantValue,
      NavigableSet<Long> activeRowIds, RowIdComparator comparator) {
    long referenceColumnValueId = comparator.findReferenceColumnValueId(column, constantValue);

    return column.getPages().values().stream().parallel(). // stream all Pages in parallel
        filter(new Predicate<ColumnPage>() { // filter out inactive Pages
          @Override
          public boolean test(ColumnPage page) {
            if (activeRowIds != null) {
              // If we're restricting the row IDs, we check if the page contains any row that we are interested in.
              Long interestedRowId = activeRowIds.ceiling(page.getFirstRowId());
              if (interestedRowId == null || interestedRowId > page.getFirstRowId() + page.size())
                return false;
            }

            return comparator.pageContainsAnyRelevantValue(page, referenceColumnValueId);
          }
        }).map(new Function<ColumnPage, Pair<ColumnPage, Set<Long>>>() { // find ColumnPageValue IDs that match the
                                                                         // comparison
          @Override
          public Pair<ColumnPage, Set<Long>> apply(ColumnPage page) {
            Set<Long> pageValueIds = comparator.findActivePageValueIds(page, referenceColumnValueId);
            return new Pair<>(page, pageValueIds);
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
   * Traverses the pages of two columns and finds rowIDs where the values of the two columns match the comparsion.
   * 
   * @param leftColumn
   *          The left column of the equation.
   * @param rightColumn
   *          The right column of the equation.
   * @param activeRowIds
   *          Set of active row IDs, used to filter the column pages. Can be <code>null</code>.
   * @param comparator
   *          The comparator implementing >, >=, < or <=.
   */
  private void executeOnOtherCol(StandardColumnShard leftColumn, StandardColumnShard rightColumn,
      NavigableSet<Long> activeRowIds, RowIdComparator comparator) {

    NavigableMap<Long, Long> comparisonMap = comparator.calculateComparisonMap(leftColumn, rightColumn);

    Long[] colValueIds1 = comparisonMap.keySet().stream().sorted().toArray(l -> new Long[l]);

    Stream<Long> resultRowIdStream = leftColumn.getPages().values().stream().parallel().
        // filter out pairs that either do not match the rowID range or where the left page does not contain any
        // interesting value
    filter(new Predicate<ColumnPage>() {
      @Override
      public boolean test(ColumnPage leftColPage) {
        if (activeRowIds != null) {
          // If we're restricting the row IDs, we check if the page contains any row that we are interested in.
          Long interestedRowId = activeRowIds.ceiling(leftColPage.getFirstRowId());
          if (interestedRowId == null || interestedRowId > leftColPage.getFirstRowId() + leftColPage.size())
            return false;
        }

        if (!leftColPage.getColumnPageDict().containsAnyValue(colValueIds1))
          return false;

        return true;
      }
    }).flatMap(new Function<ColumnPage, Stream<Long>>() {

      @Override
      public Stream<Long> apply(ColumnPage leftColPage) {
        // resolve ColumnPage value IDs from column value IDs for left page for all column value IDs we're
        // interested in.
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
      }

    });

    sendRowIds(resultRowIdStream);
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
     */
    public <T> long findReferenceColumnValueId(ColumnShard column, Object value);

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
     */
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        StandardColumnShard rightCol);

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
    public boolean rowMatches(long leftColumnValueId, long rowId, ColumnShard rightCol, Map<Long, Long> comparisonMap);
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
    public <T> long findReferenceColumnValueId(ColumnShard column, Object value) {
      long grEqId;

      try {
        grEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findGtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }
      // ignore positive/negative encoding
      if (grEqId < 0)
        grEqId = -(grEqId + 1);
      return grEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        StandardColumnShard rightCol) {
      return ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findGtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, ColumnShard rightCol, Map<Long, Long> comparisonMap) {
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
    public <T> long findReferenceColumnValueId(ColumnShard column, Object value) {
      // search less than or equal ID, when doing a > search later, this will find us the right results.
      long ltEqId;
      try {
        ltEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findLtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }

      // ignore positive/negative encoding
      if (ltEqId < 0)
        ltEqId = -(ltEqId + 1);

      return ltEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        StandardColumnShard rightCol) {
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
    public boolean rowMatches(long leftColumnValueId, long rowId, ColumnShard rightCol, Map<Long, Long> comparisonMap) {
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
    public <T> long findReferenceColumnValueId(ColumnShard column, Object value) {
      long ltEq;
      try {
        ltEq = ((Dictionary<T>) column.getColumnShardDictionary()).findLtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }

      // ignore positive/negative encoding.
      if (ltEq < 0)
        ltEq = -(ltEq + 1);
      return ltEq;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        StandardColumnShard rightCol) {
      return ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findLtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());
    }

    @Override
    public boolean rowMatches(long leftColumnValueId, long rowId, ColumnShard rightCol, Map<Long, Long> comparisonMap) {
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
    public <T> long findReferenceColumnValueId(ColumnShard column, Object value) {
      // find the ID of the next >= value compared to the requested one. When executing a strong < comparison later,
      // this will return the correct results.
      long grEqId;
      try {
        grEqId = ((Dictionary<T>) column.getColumnShardDictionary()).findGtEqIdOfValue((T) value);
      } catch (ClassCastException e) {
        throw new ExecutablePlanExecutionException(
            "Cannot compare column " + column.getName() + " with value of type " + value.getClass().getSimpleName());
      }

      // ignore positive/negative result encoding.
      if (grEqId < 0)
        grEqId = -(grEqId + 1);

      return grEqId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> NavigableMap<Long, Long> calculateComparisonMap(StandardColumnShard leftCol,
        StandardColumnShard rightCol) {
      // start off with a 'less than or equal' comparison map
      NavigableMap<Long, Long> res = ((Dictionary<T>) leftCol.getColumnShardDictionary())
          .findLtEqIds((Dictionary<T>) rightCol.getColumnShardDictionary());

      for (Iterator<Entry<Long, Long>> it = res.entrySet().iterator(); it.hasNext();) {
        Entry<Long, Long> e = it.next();
        if (e.getValue() == rightCol.getFirstRowId() + rightCol.getNumberOfRowsInColumnShard() - 1)
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
    public boolean rowMatches(long leftColumnValueId, long rowId, ColumnShard rightCol, Map<Long, Long> comparisonMap) {
      long rightColumnValueId = rightCol.resolveColumnValueIdForRow(rowId);
      return (rightColumnValueId != -1 && rightColumnValueId >= comparisonMap.get(leftColumnValueId));
    }
  }
}
