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
package org.diqube.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupFinalAggregationConsumer;
import org.diqube.execution.consumers.AbstractThreadedOrderedRowIdConsumer;
import org.diqube.plan.ExecutionPlanBuilder;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.exception.ValidationException;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.query.thrift.RResultTable;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Fully executes a diql query and provides a callback that has a {@link RResultTable} on a query master.
 * 
 * <p>
 * One instance of this class can be used only for executing one single query.
 *
 * @author Bastian Gloeckle
 */
class MasterQueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(MasterQueryExecutor.class);

  private MasterQueryExecutor.QueryExecutorCallback callback;
  private ExecutionPlanBuilderFactory executionPlanBuildeFactory;

  private volatile Map<Long, Map<String, Object>> valuesByRow = new ConcurrentHashMap<>();
  private List<Long> orderedRowIds;
  private Object orderedSync = new Object();

  private Object waiter = new Object();

  private AtomicBoolean valuesDone = new AtomicBoolean(false);
  private AtomicBoolean groupsDone = new AtomicBoolean(false);
  private AtomicBoolean orderedDone = new AtomicBoolean(false);
  private AtomicInteger updatesWaiting = new AtomicInteger(0);
  private Set<String> selectedColumnsSet;
  private boolean isGrouped;
  private boolean isOrdered;
  private List<String> selectedColumns;
  private boolean createIntermediaryUpdates;

  private ExecutorManager executorManager;

  public MasterQueryExecutor(ExecutorManager executorManager, ExecutionPlanBuilderFactory executionPlanBuildeFactory,
      MasterQueryExecutor.QueryExecutorCallback callback, boolean createIntermediaryUpdates) {
    this.executorManager = executorManager;
    this.executionPlanBuildeFactory = executionPlanBuildeFactory;
    this.callback = callback;
    this.createIntermediaryUpdates = createIntermediaryUpdates;
  }

  /**
   * Prepares executing the given query.
   * 
   * <p>
   * The returned {@link Runnable} will, when called, execute the query, block the current thread until the execution is
   * done and will call the callback accordingly.
   * 
   * @throws ParseException
   *           in case the query cannot be parsed.
   * @throws ValidationException
   *           in case the query cannot be validated.
   */
  public Runnable prepareExecution(UUID queryUuid, UUID executionUuid, String diql)
      throws ParseException, ValidationException {
    ExecutionPlanBuilder planBuilder = executionPlanBuildeFactory.createExecutionPlanBuilder();
    planBuilder.fromDiql(diql);
    planBuilder.withFinalColumnValueConsumer(new AbstractThreadedColumnValueConsumer(null) {
      @Override
      protected void allSourcesAreDone() {
        valuesDone.set(true);
        scheduleUpdate();
      }

      @Override
      protected void doConsume(String colName, Map<Long, Object> values) {
        for (Entry<Long, Object> valueEntry : values.entrySet()) {
          if (!valuesByRow.containsKey(valueEntry.getKey())) {
            synchronized (valuesByRow) {
              if (!valuesByRow.containsKey(valueEntry.getKey())) {
                valuesByRow.put(valueEntry.getKey(), new ConcurrentHashMap<>());
              }
            }
          }

          valuesByRow.get(valueEntry.getKey()).put(colName, valueEntry.getValue());
        }
        scheduleUpdate();
      }
    });

    planBuilder.withFinalGroupFinalAggregationConsumer(new AbstractThreadedGroupFinalAggregationConsumer(null) {

      @Override
      protected void allSourcesAreDone() {
        groupsDone.set(true);
        scheduleUpdate();
      }

      @Override
      protected void doConsumeAggregationResult(long groupId, String colName, Object result) {
        if (!valuesByRow.containsKey(groupId)) {
          synchronized (valuesByRow) {
            if (!valuesByRow.containsKey(groupId)) {
              valuesByRow.put(groupId, new ConcurrentHashMap<>());
            }
          }
        }

        valuesByRow.get(groupId).put(colName, result);
        scheduleUpdate();
      }
    });

    planBuilder.withFinalOrderedRowIdConsumer(new AbstractThreadedOrderedRowIdConsumer(null) {

      @Override
      protected void allSourcesAreDone() {
        orderedDone.set(true);
        scheduleUpdate();
      }

      @Override
      protected void doConsumeOrderedRowIds(List<Long> rowIds) {
        synchronized (orderedSync) {
          orderedRowIds = rowIds;
        }
        scheduleUpdate();
      }
    });

    ExecutablePlan plan = planBuilder.build();
    selectedColumnsSet = new HashSet<>(plan.getInfo().getSelectedColumnNames());
    selectedColumns = plan.getInfo().getSelectedColumnNames();
    isGrouped = plan.getInfo().isGrouped();
    isOrdered = plan.getInfo().isOrdered();
    if (!isGrouped)
      groupsDone.set(true);
    if (!isOrdered)
      orderedDone.set(true);

    return new Runnable() {
      @Override
      public void run() {
        Executor executor = executorManager.newQueryFixedThreadPool(plan.preferredExecutorServiceSize(),
            "query-master-worker-" + queryUuid + "-%d", //
            queryUuid, executionUuid);

        Future<Void> planFuture = plan.executeAsynchronously(executor);

        processUntilPlanIsExecuted(planFuture);
      }
    };
  }

  /**
   * Keeps waiting for updates, potentially generates partial udpates of results and will work until all data has been
   * provided by the source steps and the final data update has been sent to the callback.
   */
  private void processUntilPlanIsExecuted(Future<Void> planFuture) {
    while (true) {
      boolean thereAreUpdates = false;
      try {
        synchronized (waiter) { // check for 0 and then wait to not block unneccesarily, see #scheduleUpdate.
          if (updatesWaiting.get() == 0) {
            waiter.wait(1000);

            // Check if there are updates and re-set counter to 0.
            // This is not 100% thread safe, but should be good enough - some updates might arrive after we set the
            // flag and we would include those updates in the resultTable, although the counter is still increased
            // - in that case we might ending up sending the same values twice. But that is better than loosing
            // updates
            // when setting the counter to 0 after creating the resultTable.
            // On the other hand, it is safe to set the counter to 0 here, as we guarantee that we'll work on all the
            // updates that are available right now. If we'd set the counter to 0 later on, on the other hand, we
            // might
            // loose updates - we're safe to not loose any updates if we do this re-setting inside the sync block.
            thereAreUpdates = updatesWaiting.getAndSet(0) != 0;
          }
        }
      } catch (InterruptedException e) {
        // we were interrupted, let's quietly shut down this thread.
        logger.trace("Interrupted while waiting for plan to be executed", e);
        return;
      }

      if (valuesDone.get() && orderedDone.get() && groupsDone.get() && planFuture.isDone()) {
        callback.finalResultTableAvailable(createRResultTableFromCurrentValues());
        return;
      }

      if (createIntermediaryUpdates && thereAreUpdates) {
        RResultTable table = createRResultTableFromCurrentValues();
        if (table != null)
          callback.intermediaryResultTableAvailable(table);
      }
    }
  }

  /**
   * Call this method when output data from the executing steps has been updated and it should be processed further (=
   * send out updates to the callback).
   */
  private void scheduleUpdate() {
    synchronized (waiter) {
      updatesWaiting.incrementAndGet(); // first increment to not wait unnecesarily in #processUntilPlanIsExecuted.
      waiter.notifyAll();
    }
  }

  /**
   * @return A {@link RResultTable} instance of the data that is currently available in {@link #valuesByRow} etc.
   */
  private RResultTable createRResultTableFromCurrentValues() {
    List<Long> rowIds = null;
    synchronized (orderedSync) {
      if (isOrdered && orderedRowIds != null)
        rowIds = new ArrayList<Long>(orderedRowIds);
    }

    if ((rowIds == null || rowIds.isEmpty()) && isOrdered)
      return null;

    RResultTable res = new RResultTable();
    res.setColumnNames(selectedColumns);

    if (rowIds == null || rowIds.isEmpty())
      rowIds = new ArrayList<>(valuesByRow.keySet()); // TODO lines will jump?

    List<List<RValue>> rows = new ArrayList<>();
    for (Long rowId : rowIds) {
      if (!valuesByRow.containsKey(rowId))
        continue;
      if (!Sets.difference(valuesByRow.get(rowId).keySet(), selectedColumnsSet).isEmpty())
        // not values for all columns available.
        continue;

      List<RValue> row = new ArrayList<>();
      for (String colName : selectedColumns)
        row.add(RValueUtil.createRValue(valuesByRow.get(rowId).get(colName)));

      // store row if we found actual values for each col. This is just some sanity checking so that we do not pass on
      // null values to thrift in the end...
      // The value seemed to be null sometimes, although we're using a ConcurrentHashMap.
      if (!row.stream().anyMatch(r -> r == null))
        rows.add(row);
    }
    res.setRows(rows);

    return res;
  }

  public static interface QueryExecutorCallback {
    /**
     * An intermediary version of the result table is available.
     * 
     * @param resultTable
     *          may be <code>null</code>.
     */
    public void intermediaryResultTableAvailable(RResultTable resultTable);

    /**
     * The final version of the result table is available.
     */
    public void finalResultTableAvailable(RResultTable resultTable);
  }
}