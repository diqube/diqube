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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * As Group IDs are valid for one TableShard only, they need to be mapped to the group IDs of equal groups from other
 * cluster nodes when receiving updates on the query master. This step does that and provides the cleaned list of
 * groupIds (= row IDs) as {@link RowIdConsumer} output.
 * 
 * <p>
 * Input: {@link ColumnValueConsumer}, {@link GroupIntermediaryAggregationConsumer}<br>
 * Output: {@link GroupIntermediaryAggregationConsumer}, {@link RowIdConsumer}
 *
 * @author Bastian Gloeckle
 */
public class GroupIdAdjustingStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(GroupIdAdjustingStep.class);

  private volatile ConcurrentMap<Long, Map<String, Object>> incomingGroupIdToValues = new ConcurrentHashMap<>();
  private AtomicBoolean columnValueSourceIsDone = new AtomicBoolean(false);

  private AbstractThreadedColumnValueConsumer columnValueConsumer = new AbstractThreadedColumnValueConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      GroupIdAdjustingStep.this.columnValueSourceIsDone.set(true);
    }

    @Override
    protected void doConsume(String colName, Map<Long, Object> values) {
      for (Entry<Long, Object> valueEntry : values.entrySet()) {
        Map<String, Object> valueMap =
            incomingGroupIdToValues.computeIfAbsent(valueEntry.getKey(), l -> new ConcurrentHashMap<String, Object>());

        valueMap.put(colName, valueEntry.getValue());
      }
    }
  };

  /** sync additions/removals by value of {@link #incomingGroupIntermediariesSync}. */
  private volatile ConcurrentMap<Long, Deque<Triple<String, IntermediaryResult, IntermediaryResult>>> incomingGroupIntermediaries =
      new ConcurrentHashMap<>();

  private AtomicBoolean groupInputIsDone = new AtomicBoolean(false);

  private ConcurrentMap<Long, Object> incomingGroupIntermediariesSync = new ConcurrentHashMap<>();

  private AbstractThreadedGroupIntermediaryAggregationConsumer groupIntermediateAggregateConsumer =
      new AbstractThreadedGroupIntermediaryAggregationConsumer(this) {
        @Override
        protected void allSourcesAreDone() {
          GroupIdAdjustingStep.this.groupInputIsDone.set(true);
        }

        @Override
        protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
            IntermediaryResult oldIntermediaryResult, IntermediaryResult newIntermediaryResult) {
          incomingGroupIntermediariesSync.putIfAbsent(groupId, new Object());

          synchronized (incomingGroupIntermediariesSync.get(groupId)) {
            incomingGroupIntermediaries.compute(groupId, (key, value) -> {
              if (value == null)
                value = new ConcurrentLinkedDeque<Triple<String, IntermediaryResult, IntermediaryResult>>();
              value.addLast(new Triple<>(colName, oldIntermediaryResult, newIntermediaryResult));
              return value;
            });
          }
        }
      };

  private Set<String> groupedColumnNames;
  private Map<Long, Long> groupIdMap = new HashMap<>();
  private Map<Map<String, Object>, Long> valuesToGroupId = new HashMap<>();
  private Set<Long> allKnownGroupIds = new HashSet<>();

  public GroupIdAdjustingStep(int stepId, QueryRegistry queryRegistry, Set<String> groupedColumnNames) {
    super(stepId, queryRegistry);
    this.groupedColumnNames = groupedColumnNames;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof GroupIntermediaryAggregationConsumer)
        && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only GroupIntermediaryAggregationConsumer and RowIdConsumer supported.");
  }

  @Override
  protected void execute() {
    execute(true);
  }

  private void execute(boolean checkIfDone) {
    if (!incomingGroupIdToValues.isEmpty()) {
      incomingGroupIdToValues.keySet().removeAll(allKnownGroupIds);
      List<Long> newGroupIds = new ArrayList<>();

      List<Long> incomingGroupIds =
          new ArrayList<Long>(Sets.difference(incomingGroupIdToValues.keySet(), allKnownGroupIds));
      incomingGroupIdToValues.keySet().removeAll(allKnownGroupIds);
      List<Long> groupIdsWorkedOn = new ArrayList<Long>();
      for (Long groupId : incomingGroupIds) {
        Map<String, Object> values = incomingGroupIdToValues.get(groupId);
        if (Sets.difference(groupedColumnNames, values.keySet()).isEmpty()) {
          values =
              Maps.filterKeys(new HashMap<String, Object>(values), colName -> groupedColumnNames.contains(colName));
          if (valuesToGroupId.containsKey(values)) {
            // we found a new groupId mapping!
            long availableGroupId = valuesToGroupId.get(values);
            groupIdMap.put(groupId, availableGroupId);
            logger.trace("Mapping new group ID {} to group ID {}", groupId, availableGroupId);
          } else {
            // new group found
            valuesToGroupId.put(values, groupId);
            groupIdMap.put(groupId, groupId);
            newGroupIds.add(groupId);
            logger.trace("Found new group ID {}", groupId);
          }
          groupIdsWorkedOn.add(groupId);
        }
      }
      for (Long groupIdDone : groupIdsWorkedOn) {
        incomingGroupIdToValues.remove(groupIdDone);
        allKnownGroupIds.add(groupIdDone);
      }

      if (!newGroupIds.isEmpty())
        forEachOutputConsumerOfType(RowIdConsumer.class,
            c -> c.consume(newGroupIds.stream().toArray(l -> new Long[l])));
    }

    processIncomingGroupIntermediaries();

    if (checkIfDone) {
      if ((groupInputIsDone.get() && isEmpty(incomingGroupIntermediaries)) || // all groups processed.
      // all inputs done, we though might not have processed everything yet.
      (groupInputIsDone.get() && columnValueSourceIsDone.get())) {

        if (groupInputIsDone.get() && columnValueSourceIsDone.get())
          // make sure we have processed everything, so lets execute one additional time.
          execute(false);

        forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
        doneProcessing();
      }
    }
  }

  private void processIncomingGroupIntermediaries() {
    if (!isEmpty(incomingGroupIntermediaries)) {
      List<Long> activeGroupIds =
          new ArrayList<>(Sets.intersection(groupIdMap.keySet(), incomingGroupIntermediaries.keySet()));
      for (Long inputGroupId : activeGroupIds) {
        long newGroupId = groupIdMap.get(inputGroupId);

        Deque<Triple<String, IntermediaryResult, IntermediaryResult>> intermediaries =
            incomingGroupIntermediaries.get(inputGroupId);

        if (intermediaries.isEmpty()) {
          synchronized (incomingGroupIntermediariesSync.get(inputGroupId)) {
            // double-checked locking since there might have been something added to the deque in the meantime.
            if (intermediaries.isEmpty()) {
              incomingGroupIntermediaries.remove(inputGroupId);
              continue;
            }
          }
        }

        logger.trace("Processing collected changes for group {}", newGroupId);
        List<String> colNamesProcessed = new ArrayList<>();
        while (!intermediaries.isEmpty()) {
          Triple<String, IntermediaryResult, IntermediaryResult> update = intermediaries.poll();

          colNamesProcessed.add(update.getLeft());

          forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class,
              c -> c.consumeIntermediaryAggregationResult(newGroupId, update.getLeft(), update.getMiddle(),
                  update.getRight()));
        }
        logger.trace("Processed collected changes for group {}, there were updates for cols {}", newGroupId,
            colNamesProcessed);
      }
    }
  }

  private boolean isEmpty(Map<Long, Deque<Triple<String, IntermediaryResult, IntermediaryResult>>> map) {
    for (Deque<Triple<String, IntermediaryResult, IntermediaryResult>> deque : map.values()) {
      if (!deque.isEmpty())
        return false;
    }
    return true;
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(
        Arrays.asList(new GenericConsumer[] { columnValueConsumer, groupIntermediateAggregateConsumer }));
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

}
