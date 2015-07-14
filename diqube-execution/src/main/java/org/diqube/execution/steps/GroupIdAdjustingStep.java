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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.function.IntermediaryResult;
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

  private volatile Map<Long, Map<String, Object>> incomingGroupIdToValues = new ConcurrentHashMap<>();
  private AtomicBoolean columnValueSourceIsDone = new AtomicBoolean(false);

  private AbstractThreadedColumnValueConsumer columnValueConsumer = new AbstractThreadedColumnValueConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      GroupIdAdjustingStep.this.columnValueSourceIsDone.set(true);
    }

    @Override
    protected void doConsume(String colName, Map<Long, Object> values) {
      for (Entry<Long, Object> valueEntry : values.entrySet()) {
        if (!incomingGroupIdToValues.containsKey(valueEntry.getKey())) {
          synchronized (incomingGroupIdToValues) {
            if (!incomingGroupIdToValues.containsKey(valueEntry.getKey()))
              incomingGroupIdToValues.put(valueEntry.getKey(), new ConcurrentHashMap<String, Object>());
          }
        }

        incomingGroupIdToValues.get(valueEntry.getKey()).put(colName, valueEntry.getValue());
      }
    }
  };

  private volatile Map<Long, Deque<Triple<String, IntermediaryResult<Object, Object, Object>, IntermediaryResult<Object, Object, Object>>>> incomingGroupIntermediaries =
      new ConcurrentHashMap<>();

  private AtomicBoolean groupInputIsDone = new AtomicBoolean(false);

  private AbstractThreadedGroupIntermediaryAggregationConsumer groupIntermediateAggregateConsumer =
      new AbstractThreadedGroupIntermediaryAggregationConsumer(this) {
        @Override
        protected void allSourcesAreDone() {
          GroupIdAdjustingStep.this.groupInputIsDone.set(true);
        }

        @Override
        protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
            IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
            IntermediaryResult<Object, Object, Object> newIntermediaryResult) {
          if (!incomingGroupIntermediaries.containsKey(groupId)) {
            synchronized (incomingGroupIntermediaries) {
              if (!incomingGroupIntermediaries.containsKey(groupId))
                incomingGroupIntermediaries.put(groupId, new ConcurrentLinkedDeque<>());
            }
          }

          incomingGroupIntermediaries.get(groupId)
              .addLast(new Triple<>(colName, oldIntermediaryResult, newIntermediaryResult));
        }
      };

  private Set<String> groupedColumnNames;
  private Map<Long, Long> groupIdMap = new HashMap<>();
  private Map<Map<String, Object>, Long> valuesToGroupId = new HashMap<>();

  public GroupIdAdjustingStep(int stepId, Set<String> groupedColumnNames) {
    super(stepId);
    this.groupedColumnNames = groupedColumnNames;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof GroupIntermediaryAggregationConsumer) && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only GroupIntermediaryAggregationConsumer and RowIdConsumer supported.");
  }

  @Override
  protected void execute() {
    if (!incomingGroupIdToValues.isEmpty()) {
      List<Long> newGroupIds = new ArrayList<>();

      List<Long> incomingGroupIds = new ArrayList<Long>(incomingGroupIdToValues.keySet());
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
      for (Long groupIdDone : groupIdsWorkedOn)
        incomingGroupIdToValues.remove(groupIdDone);

      if (!newGroupIds.isEmpty())
        forEachOutputConsumerOfType(RowIdConsumer.class,
            c -> c.consume(newGroupIds.stream().toArray(l -> new Long[l])));
    }

    processIncomingGroupIntermediaries();

    if ((groupInputIsDone.get() && isEmpty(incomingGroupIntermediaries)) || // all groups processed.
    // all column values processed - it could happen that we receive values for fewer rowIds than we receive group
    // information for, as the rowIds may have been cut-off (order!) after calculating group intermediaries.
    (groupInputIsDone.get() && columnValueSourceIsDone.get() && incomingGroupIdToValues.isEmpty())) {

      if ((groupInputIsDone.get() && columnValueSourceIsDone.get() && incomingGroupIdToValues.isEmpty())
          && !isEmpty(incomingGroupIntermediaries))
        // if column value source is done and there are no column values to process any more, but there are still some
        // incoming group intermediates left, make absolutely sure that we processed all of them at least once. This is
        // needed, if all column values have been provided when starting to run execute(), but the last group
        // intermediates were received right before executing the 'if' above. Then we might loose some group
        // information.
        processIncomingGroupIntermediaries();

      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  private void processIncomingGroupIntermediaries() {
    if (!isEmpty(incomingGroupIntermediaries)) {
      List<Long> activeGroupIds =
          new ArrayList<>(Sets.intersection(groupIdMap.keySet(), incomingGroupIntermediaries.keySet()));
      for (Long inputGroupId : activeGroupIds) {
        long newGroupId = groupIdMap.get(inputGroupId);

        logger.trace("Processing collected changes for group {}", newGroupId);
        while (!incomingGroupIntermediaries.get(inputGroupId).isEmpty()) {
          Triple<String, IntermediaryResult<Object, Object, Object>, IntermediaryResult<Object, Object, Object>> update =
              incomingGroupIntermediaries.get(inputGroupId).poll();

          forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class,
              c -> c.consumeIntermediaryAggregationResult(newGroupId, update.getLeft(), update.getMiddle(),
                  update.getRight()));
        }
      }
    }
  }

  private boolean isEmpty(
      Map<Long, Deque<Triple<String, IntermediaryResult<Object, Object, Object>, IntermediaryResult<Object, Object, Object>>>> map) {
    for (Deque<Triple<String, IntermediaryResult<Object, Object, Object>, IntermediaryResult<Object, Object, Object>>> deque : map
        .values()) {
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
