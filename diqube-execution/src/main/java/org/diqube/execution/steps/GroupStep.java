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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Input: 1 {@link RowIdConsumer}<br>
 * Output: {@link RowIdConsumer} and/or {@link GroupConsumer} and/or {@link GroupDeltaConsumer}.
 *
 * <p>
 * TODO #9 support grouping on projected columns. This would lead to having a ColumnBuiltConsumer as input
 * 
 * @author Bastian Gloeckle
 */
public class GroupStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(GroupStep.class);

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

  public GroupStep(int stepId, ExecutionEnvironment env, List<String> colNamesToGroupBy) {
    super(stepId);
    this.colNamesToGroupBy = colNamesToGroupBy;

    // resolve to column shard objects
    List<ColumnShard> columnsToGroupBy = colNamesToGroupBy.stream().map(new Function<String, ColumnShard>() {
      @Override
      public ColumnShard apply(String colName) {
        return env.getColumnShard(colName);
      }
    }).collect(Collectors.toList());

    // create groupers.
    headGrouper = createGroupers(columnsToGroupBy, 0).get();
  }

  /**
   * Create a Grouper that will do the grouping for the columns specified, starting from the specified index. That means
   * the resulting Supplier will supply a new {@link Grouper} instance that will group by all column in columnsToGroupBy
   * with index starting from the provided one.
   */
  private Supplier<Grouper> createGroupers(List<ColumnShard> columnsToGroupBy, int index) {
    return () -> {
      if (index == columnsToGroupBy.size())
        // Use a Leaf grouper after the last Non-lead grouper.
        return new Grouper();

      return new Grouper(columnsToGroupBy.get(index), createGroupers(columnsToGroupBy, index + 1));
    };
  }

  @Override
  protected void execute() {
    List<Long> activeRowIds = new ArrayList<>();
    Long newRowId;
    while ((newRowId = rowIds.poll()) != null)
      activeRowIds.add(newRowId);

    if (activeRowIds.size() > 0) {
      // use headGrouper to group the new RowIDs, collect the new groupings in a new map.
      Map<Long, List<Long>> changesGroups = new HashMap<>();
      headGrouper.groupRowIds(activeRowIds, changesGroups);

      logger.trace("Grouped new rowIds: {}", changesGroups);

      Set<Long> newGroupIds = Sets.difference(changesGroups.keySet(), groups.keySet());

      if (!newGroupIds.isEmpty()) {
        // If we started new groups, we need to resolve the values of the group-by fields (if they are selected, e.g.).
        // As each groupID is in fact a rowID (of one arbitrary row that is inside the group), we find those new row IDs
        // and send them to RowID consumers.
        Long[] newRowIdsArray = newGroupIds.stream().toArray(l -> new Long[l]);
        logger.trace("New group IDs: {}", newRowIdsArray);

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
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof RowIdConsumer) && !(consumer instanceof GroupConsumer)
        && !(consumer instanceof GroupDeltaConsumer))
      throw new IllegalArgumentException("Only RowIdConsumer, GroupConsumer and GroupDeltaConsumer accepted.");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer });
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
    private ColumnShard column;
    private Map<Long, Grouper> delegateGroupers;
    private Long groupId = null;
    private boolean isLeaf;
    private Supplier<Grouper> delegateGroupersFactory;

    public Grouper(ColumnShard column, Supplier<Grouper> delegateGroupersFactory) {
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

      Map<Long, List<Long>> columnValueToRowIds = new HashMap<>();

      // Group row IDs by their ColumnPage and then use dict of ColumnPage to resolve Column Value IDs for these rows.
      Map<ColumnPage, List<Long>> pageToRowIds =
          rowIds.stream().collect(Collectors.groupingBy(new Function<Long, ColumnPage>() {
            @Override
            public ColumnPage apply(Long t) {
              NavigableMap<Long, ColumnPage> pages = ((StandardColumnShard) column).getPages();
              return pages.get(pages.floorKey(t));
            }
          }));
      pageToRowIds.forEach(new BiConsumer<ColumnPage, List<Long>>() {
        @Override
        public void accept(ColumnPage page, List<Long> rowIdList) {
          Long[] rowIds = rowIdList.toArray(new Long[rowIdList.size()]);

          // TODO #7 perhaps decompress whole value array, as it may be RLE encoded anyway.
          Long[] pageValueIds = Stream.<Long> of(rowIds)
              .map(rowId -> page.getValues().get((int) (rowId - page.getFirstRowId()))).toArray(l -> new Long[l]);

          Long[] columnValueIds = page.getColumnPageDict().decompressValues(pageValueIds);

          for (int i = 0; i < rowIds.length; i++) {
            Long columnValueId = columnValueIds[i];
            Long rowId = rowIds[i];

            if (!columnValueToRowIds.containsKey(columnValueId))
              columnValueToRowIds.put(columnValueId, new ArrayList<>());
            columnValueToRowIds.get(columnValueId).add(rowId);
          }
        }
      });

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
