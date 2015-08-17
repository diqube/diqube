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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Removes all not explicitly requested columns from the result values and removes all result rows whose rowID is not
 * provided by a specific {@link RowIdConsumer}.
 * 
 * <p>
 * The filtering of columns is needed, because when executing an execution plan remotely, the query master might (and
 * probably will) request the values of additional columns that were not explicitly requested by the user. This is
 * needed in order that the query master can fulfill the needed orderings etc. This step filters out the unrequested
 * result values in order to provide a clean dataset to the user.
 * 
 * <p>
 * Removing rowIds from the result is needed, too, in case the query master did group Id adjustments (
 * {@link GroupIdAdjustingStep}) - in that caase the {@link ColumnValueConsumer} on the
 * {@link ExecuteRemotePlanOnShardsStep} step might have reported too many rows.
 *
 * <p>
 * Input: 1 {@link RowIdConsumer}, >= 1 {@link ColumnValueConsumer}<br>
 * Output: {@link ColumnValueConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class FilterRequestedColumnsAndActiveRowIdsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(FilterRequestedColumnsAndActiveRowIdsStep.class);

  private AtomicBoolean valueSourcesAreDone = new AtomicBoolean(false);

  private Deque<Pair<String, Map<Long, Object>>> incomingValues = new ConcurrentLinkedDeque<>();

  private AbstractThreadedColumnValueConsumer columnValueConsumer = new AbstractThreadedColumnValueConsumer(this) {

    @Override
    protected void allSourcesAreDone() {
      FilterRequestedColumnsAndActiveRowIdsStep.this.valueSourcesAreDone.set(true);
    }

    @Override
    protected void doConsume(String colName, Map<Long, Object> values) {
      if (!requestedColumns.contains(colName))
        return;

      incomingValues.add(new Pair<>(colName, values));
    }
  };

  private AtomicBoolean rowIdSourcesAreDone = new AtomicBoolean(false);
  private Deque<Long> incomingRowIds = new ConcurrentLinkedDeque<>();
  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      FilterRequestedColumnsAndActiveRowIdsStep.this.rowIdSourcesAreDone.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      incomingRowIds.addAll(Stream.of(rowIds).collect(Collectors.toList()));
    }
  };

  private Set<String> requestedColumns;
  private Set<Long> allRowIds = new HashSet<>();
  private Map<String, Map<Long, Object>> allValues = new HashMap<>();

  public FilterRequestedColumnsAndActiveRowIdsStep(int stepId, QueryRegistry queryRegistry,
      Set<String> requestedColumns) {
    super(stepId, queryRegistry);
    this.requestedColumns = requestedColumns;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof ColumnValueConsumer))
      throw new IllegalArgumentException("Only ColumnValueConsumer accepted.");
  }

  @Override
  protected void execute() {
    if (!incomingRowIds.isEmpty()) {
      Set<Long> newRowIds = new HashSet<Long>();
      Long tmp;
      while ((tmp = incomingRowIds.poll()) != null)
        newRowIds.add(tmp);

      processValues(allValues, newRowIds);

      allRowIds.addAll(newRowIds);
    }

    if (!incomingValues.isEmpty()) {
      Map<String, Map<Long, Object>> activeValues = new HashMap<>();
      Pair<String, Map<Long, Object>> incomingPair;
      while ((incomingPair = incomingValues.poll()) != null) {
        if (!activeValues.containsKey(incomingPair.getLeft()))
          activeValues.put(incomingPair.getLeft(), new HashMap<Long, Object>());
        activeValues.get(incomingPair.getLeft()).putAll(incomingPair.getRight());
      }

      processValues(activeValues, allRowIds);

      for (Entry<String, Map<Long, Object>> newEntry : activeValues.entrySet()) {
        if (!allValues.containsKey(newEntry.getKey()))
          allValues.put(newEntry.getKey(), new HashMap<Long, Object>());
        allValues.get(newEntry.getKey()).putAll(newEntry.getValue());
      }
    }

    if (valueSourcesAreDone.get() && rowIdSourcesAreDone.get() && incomingRowIds.isEmpty()
        && incomingValues.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  /**
   * Filters all values of all columns that have one of the specified rowIds and informs {@link ColumnValueConsumer}s
   * about them.
   */
  private void processValues(Map<String, Map<Long, Object>> values, Set<Long> rowIds) {
    for (Entry<String, Map<Long, Object>> valueEntry : values.entrySet()) {
      Set<Long> activeValueRowIds = Sets.intersection(valueEntry.getValue().keySet(), rowIds);
      if (!activeValueRowIds.isEmpty()) {
        Map<Long, Object> newValues =
            Maps.filterKeys(valueEntry.getValue(), rowId -> activeValueRowIds.contains(rowId));

        logger.trace("Sending out values for {}, rowIds (limit) {}", valueEntry.getKey(),
            Iterables.limit(activeValueRowIds, 100));

        forEachOutputConsumerOfType(ColumnValueConsumer.class, c -> c.consume(valueEntry.getKey(), newValues));
      }
    }
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { columnValueConsumer, rowIdConsumer }));
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "requestedCols=" + requestedColumns;
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    if (columnValueConsumer.getNumberOfTimesWired() == 0 || rowIdConsumer.getNumberOfTimesWired() != 1)
      throw new ExecutablePlanBuildException("Input not wired.");
  }

}
