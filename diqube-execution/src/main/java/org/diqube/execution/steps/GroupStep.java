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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Executes a GROUP BY clause.
 * 
 * <p>
 * As each group that is produced will end up to be one row in the overall result table of the query, we choose one row
 * ID per group that identifies the group . This identifying rowID is also called the "group id". This group ID though
 * is not identifying the group globally, but only on one {@link TableShard}, as other table shards will choose other
 * row IDs that may reference the same group (same group = group with the same values in the group-by-fields).
 * <p>
 * There are three output consumers that are fed with data by this step: {@link RowIdConsumer}s that will be fed with
 * the groupIDs/identifying row IDs (= can be used to resolve any values that need to be resolved for a group). In
 * addition to that the {@link GroupConsumer}s and {@link GroupDeltaConsumer}s will be fed with the actual grouping of
 * the row IDs.
 * 
 * <p>
 * The columns which should be grouped by are expected to be {@link StandardColumnShard}s.
 * 
 * <p>
 * Input: 1 {@link RowIdConsumer}, 1 optional {@link ColumnBuiltConsumer} <br>
 * Output: {@link RowIdConsumer} and/or {@link GroupConsumer} and/or {@link GroupDeltaConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class GroupStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(GroupStep.class);

  private AtomicBoolean allColumnsBuilt = new AtomicBoolean(false);
  private Set<String> columnsThatNeedToBeBuilt;
  private AbstractThreadedColumnBuiltConsumer columnBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
    }

    @Override
    protected void doColumnBuilt(String colName) {
      columnsThatNeedToBeBuilt.remove(colName);

      if (columnsThatNeedToBeBuilt.isEmpty())
        allColumnsBuilt.set(true);
    }
  };

  private AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> rowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      GroupStep.this.sourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        GroupStep.this.rowIds.add(rowId);
    }
  };

  /**
   * The {@link Grouper} that controls all the groupings. If the grouping should be made by multiple fields, this
   * grouper will automatically take care of that.
   */
  private Grouper headGrouper;

  private Map<Long, List<Long>> groups = new HashMap<>();
  private List<String> colNamesToGroupBy;

  private ExecutionEnvironment defaultEnv;

  public GroupStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment env, List<String> colNamesToGroupBy) {
    super(stepId, queryRegistry);
    this.defaultEnv = env;
    this.colNamesToGroupBy = colNamesToGroupBy;
  }

  @Override
  public void initialize() {
    columnsThatNeedToBeBuilt = new ConcurrentSkipListSet<>(colNamesToGroupBy);
    for (Iterator<String> it = columnsThatNeedToBeBuilt.iterator(); it.hasNext();)
      if (defaultEnv.getColumnShard(it.next()) != null)
        it.remove();
  }

  /**
   * Create a Grouper that will do the grouping for the columns specified, starting from the specified index. That means
   * the resulting Supplier will supply a new {@link Grouper} instance that will group by all column in columnsToGroupBy
   * with index starting from the provided one.
   */
  private Supplier<Grouper> createGroupers(List<String> columnsToGroupBy, int index) {
    return () -> {
      if (index == columnsToGroupBy.size())
        // Use a Leaf grouper after the last Non-lead grouper.
        return new Grouper();

      QueryableColumnShard shard = defaultEnv.getColumnShard(columnsToGroupBy.get(index));
      return new Grouper(shard, createGroupers(columnsToGroupBy, index + 1));
    };
  }

  @Override
  protected void execute() {
    if (columnBuiltConsumer.getNumberOfTimesWired() > 0 && !allColumnsBuilt.get())
      // we wait until our columns are all built.
      return;

    if (headGrouper == null)
      // create groupers. Do this just now, as we know that now really all columns are available!
      headGrouper = createGroupers(colNamesToGroupBy, 0).get();

    List<Long> activeRowIds = new ArrayList<>();
    Long newRowId;
    while ((newRowId = rowIds.poll()) != null)
      activeRowIds.add(newRowId);

    if (activeRowIds.size() > 0) {
      // use headGrouper to group the new RowIDs, collect the new groupings in a new map.
      Map<Long, List<Long>> changesGroups = new HashMap<>();
      headGrouper.groupRowIds(activeRowIds, changesGroups);

      logger.trace("Grouped new rowIds (limit each): {}",
          Maps.transformValues(changesGroups, lst -> Iterables.limit(lst, 50)));

      Set<Long> newGroupIds = Sets.difference(changesGroups.keySet(), groups.keySet());

      if (!newGroupIds.isEmpty()) {
        // If we started new groups, we need to resolve the values of the group-by fields (if they are selected, e.g.).
        // As each groupID is in fact a rowID (of one arbitrary row that is inside the group), we find those new row IDs
        // and send them to RowID consumers.
        Long[] newRowIdsArray = newGroupIds.stream().toArray(l -> new Long[l]);
        logger.trace("New group IDs (limit): {}", Iterables.limit(Arrays.asList(newRowIdsArray), 100));

        forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(newRowIdsArray));
      }

      for (Long groupId : changesGroups.keySet()) {
        if (!groups.containsKey(groupId))
          groups.put(groupId, changesGroups.get(groupId));
        else
          groups.get(groupId).addAll(changesGroups.get(groupId));
      }

      forEachOutputConsumerOfType(GroupDeltaConsumer.class, c -> c.consumeGroupDeltas(changesGroups));
      forEachOutputConsumerOfType(GroupConsumer.class, c -> c.consumeGroups(groups));
    }
    if (sourceIsEmpty.get() && rowIds.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    if (rowIdConsumer.getNumberOfTimesWired() == 0)
      throw new ExecutablePlanBuildException("RowId input not wired.");
    // ColumnBuiltConsumer does not have to be wired.
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof RowIdConsumer)
        && !(consumer instanceof GroupConsumer) && !(consumer instanceof GroupDeltaConsumer))
      throw new IllegalArgumentException("Only RowIdConsumer, GroupConsumer and GroupDeltaConsumer accepted.");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer, columnBuiltConsumer });
  }

  /**
   * A {@link Grouper} is capable of grouping row IDs by one column and additionally forward the grouping requests to
   * other groupers which will group by different columns.
   * 
   * <p>
   * Each grouper is in one of two states:
   * <ul>
   * <li>Leaf: These are the groupers that do not have any delegate groupers (= grouping on the column that was
   * specified last in the GROUP BY stmt). Leaf groupers to not actually group anyything, but identify the groupId of a
   * (new) group and record the new additions to a group. Each Leaf {@link Grouper} represents one group.
   * <li>Non-Leaf: These forward any newly incoming rowIDs by the value of that row in the given column. After these
   * rowIDs have been grouped, each group is forwarded to a delegate grouper to group it further (or, if the delegate is
   * a leaf, to record the group).
   * </ul>
   */
  private class Grouper {
    private QueryableColumnShard column;
    private Map<Long, Grouper> delegateGroupers;
    private Long groupId = null;
    private boolean isLeaf;
    private Supplier<Grouper> delegateGroupersFactory;

    public Grouper(QueryableColumnShard column, Supplier<Grouper> delegateGroupersFactory) {
      this.column = column;
      this.delegateGroupersFactory = delegateGroupersFactory;
      delegateGroupers = new HashMap<>();
      isLeaf = false;
    }

    public Grouper() {
      isLeaf = true;
    }

    public void groupRowIds(List<Long> rowIds, Map<Long, List<Long>> changes) {
      if (isLeaf) {
        if (groupId == null)
          groupId = rowIds.iterator().next();

        changes.put(groupId, rowIds);
        return;
      }

      Map<Long, Long> rowIdToColValId = column.resolveColumnValueIdsForRows(rowIds);

      Map<Long, List<Long>> columnValueToRowIds = new HashMap<>();
      for (Entry<Long, Long> e : rowIdToColValId.entrySet()) {
        long rowId = e.getKey();
        long colValueId = e.getValue();
        if (!columnValueToRowIds.containsKey(colValueId))
          columnValueToRowIds.put(colValueId, new ArrayList<>());
        columnValueToRowIds.get(colValueId).add(rowId);
      }

      // Add the row IDs to delegate groupers based on their column value id.
      columnValueToRowIds.forEach(new BiConsumer<Long, List<Long>>() {
        @Override
        public void accept(Long columnValueId, List<Long> rowIds) {
          if (!delegateGroupers.containsKey(columnValueId))
            delegateGroupers.put(columnValueId, delegateGroupersFactory.get());

          delegateGroupers.get(columnValueId).groupRowIds(rowIds, changes);
        }
      });
    }
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "colsToGroupBy=" + colNamesToGroupBy;
  }

}
