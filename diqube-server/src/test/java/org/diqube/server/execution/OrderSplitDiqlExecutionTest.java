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
package org.diqube.server.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.antlr.v4.runtime.misc.Triple;
import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.plan.planner.ExecutionPlanner;
import org.diqube.plan.planner.OrderRequestBuilder;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

/**
 * Validates ORDER BY diql queries that target for the split of the order by statement on cluster nodes and the query
 * master. Compare how {@link OrderRequestBuilder}s are used in {@link ExecutionPlanner}.
 *
 * @author Bastian Gloeckle
 */
public abstract class OrderSplitDiqlExecutionTest<T> extends AbstractCacheDoubleDiqlExecutionTest<T> {

  public OrderSplitDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Test
  public void aggregatedGroupedOrderMultiShard() throws InterruptedException, ExecutionException {
    // GIVEN
    Object[] colAShard1 = dp.a(1L, 1L, 2L, 2L, 3L, 4L);
    Object[] colBShard1 = dp.a(100L, 100L, 400L, 300L, 0L, 0L);
    Object[] colAShard2 = dp.a(1L, 2L, 2L, 2L, 2L, 2L, 3L, 4L);
    Object[] colBShard2 = dp.a(100L, 300L, 300L, 300L, 300L, 0L, 0L, 0L);

    initializeMultiShardTable(Arrays.asList(new Pair[] { new Pair<Object[], Object[]>(colAShard1, colBShard1),
        new Pair<Object[], Object[]>(colAShard2, colBShard2) }));

    ExecutablePlan executablePlan = buildExecutablePlan( //
        "Select " + COL_A + ", " + COL_B + ", count() from " + TABLE + //
            " group by " + COL_A + ", " + COL_B + //
            " order by " + COL_A + ", count() desc LIMIT 2");

    Set<Long> allRowIdsReportedByClusterNodes = new ConcurrentSkipListSet<>();
    // Add a RowId consumer to the ExecuteRemotePlanOnShardsStep in order to collect all rowIDs that have been reported
    // by cluster nodes/remote executions.
    executablePlan.getSteps().stream().filter(step -> step instanceof ExecuteRemotePlanOnShardsStep)
        .forEach(new Consumer<ExecutablePlanStep>() {
          @Override
          public void accept(ExecutablePlanStep executeRemoteStep) {
            executeRemoteStep.addOutputConsumer(new AbstractThreadedRowIdConsumer(null) {
              @Override
              protected void allSourcesAreDone() {
              }

              @Override
              protected synchronized void doConsume(Long[] rowIds) {
                allRowIdsReportedByClusterNodes.addAll(Arrays.asList(rowIds));
              }
            });
          }
        });

    ExecutorService executor = executors.newTestExecutor(executablePlan.preferredExecutorServiceSize());
    try {
      // WHEN
      // executing it on the sample table
      Future<Void> future = executablePlan.executeAsynchronously(executor);
      future.get(); // wait until done.

      // THEN
      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 3, "Result values should be available for three columns");

      // we expect a SOFT limit to be set, because the remotes cannot order fully, because they do not know the final
      // values of the group aggregated order column ("count()"). Therefore the remote has to have a soft limit set,
      // which leads the remote to potentially send more rows than the limit asks to. If the correct additional rows
      // have been reported is asserted below.
      Assert.assertTrue(findRemoteOrderStep(executablePlan).getDetailsOrder().isSetSoftLimit(),
          "Expected remote order step to have a soft limit");

      List<Triple<Object, Object, Object>> expectedValues = new ArrayList<>();
      // colA: 1L, colB: 100L, count: 3
      expectedValues.add(new Triple<>(dp.v(1), dp.v(100), 3L));
      // colA: 2L, colB: 300L, count: 5
      expectedValues.add(new Triple<>(dp.v(2), dp.v(300), 5L));

      String resultCountCol = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();

      for (int i = 0; i < expectedValues.size(); i++) {
        Triple<Object, Object, Object> expected = expectedValues.get(i);
        long rowId = resultOrderRowIds.get(i);
        Object valueColA = resultValues.get(COL_A).get(rowId);
        Object valueColB = resultValues.get(COL_B).get(rowId);
        Object valueColCount = resultValues.get(resultCountCol).get(rowId);
        Triple<Object, Object, Object> actualResult = new Triple<>(valueColA, valueColB, valueColCount);
        Assert.assertEquals(actualResult, expected,
            "Expected correct result for index " + i + " (rowId " + rowId + ")");
      }

      Assert.assertEquals(resultOrderRowIds.size(), expectedValues.size(), "Expected correct number of result rows");

      // check if correct rowIDs have been reported to query master. We check this by inspecting the values of colA and
      // colB for the rowIds that have been reported and how often those values were reported.
      // Note that we check that _at least_ a certain number of rows with the values was returned. If the timing of the
      // threads is unfortunate on the remotes, the OrderStep might get woken up before all rowIds have been reported to
      // it -> it has a smaller number of rows to order -> it might send rowIds on their journey which would not be part
      // if all the rowIds would have been available to it right away.

      Map<Pair<Object, Object>, Integer> expectedRowIdMinValues = new HashMap<>();
      expectedRowIdMinValues.clear();
      // ColA: 1, ColB: 100, number of times reported: 2
      expectedRowIdMinValues.put(new Pair<>(dp.v(1), dp.v(100)), 2);
      // ColA: 2, ColB: 400, number of times reported: 1
      expectedRowIdMinValues.put(new Pair<>(dp.v(2), dp.v(400)), 1);
      // ColA: 2, ColB: 300, number of times reported: 2
      expectedRowIdMinValues.put(new Pair<>(dp.v(2), dp.v(300)), 2);
      // ColA: 2, ColB: 0, number of times reported: 1
      expectedRowIdMinValues.put(new Pair<>(dp.v(2), dp.v(0)), 1);

      Map<Pair<Object, Object>, Integer> actualRowIdValues = new HashMap<>();
      for (Long reportedRowId : allRowIdsReportedByClusterNodes) {
        Object colAValue = (reportedRowId < colAShard1.length) ? colAShard1[reportedRowId.intValue()]
            : colAShard2[(int) (reportedRowId - colAShard1.length)];
        Object colBValue = (reportedRowId < colBShard1.length) ? colBShard1[reportedRowId.intValue()]
            : colBShard2[(int) (reportedRowId - colBShard1.length)];

        Pair<Object, Object> value = new Pair<>(colAValue, colBValue);
        if (actualRowIdValues.containsKey(value))
          actualRowIdValues.put(value, actualRowIdValues.get(value) + 1);
        else
          actualRowIdValues.put(value, 1);
      }

      Set<Pair<Object, Object>> valuesNotReported =
          Sets.difference(expectedRowIdMinValues.keySet(), actualRowIdValues.keySet());
      Assert.assertTrue(valuesNotReported.isEmpty(),
          "Expected that query master was provided with all interesting row IDs, so that it was "
              + "able to do the ordering correctly. Not reported values: " + valuesNotReported);

      for (Pair<Object, Object> p : expectedRowIdMinValues.keySet())
        Assert.assertTrue(expectedRowIdMinValues.get(p) <= actualRowIdValues.get(p),
            "Expected to receive " + p + "at least " + expectedRowIdMinValues.get(p) + " times, but did receive it "
                + actualRowIdValues.get(p) + " times.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void aggregatedOrderMultiShard() throws InterruptedException, ExecutionException {
    // GIVEN
    Object[] colAShard1 = dp.a(2L, 2L, 2L, 3L, 4L);
    Object[] colBShard1 = dp.a(300L, 400L, 300L, 0L, 0L);
    Object[] colAShard2 = dp.a(1L, 2L, 2L, 2L, 2L, 2L, 3L, 4L);
    Object[] colBShard2 = dp.a(100L, 300L, 300L, 400L, 400L, 0L, 0L, 0L);

    initializeMultiShardTable(Arrays.asList(new Pair[] { new Pair<Object[], Object[]>(colAShard1, colBShard1),
        new Pair<Object[], Object[]>(colAShard2, colBShard2) }));

    ExecutablePlan executablePlan = buildExecutablePlan( //
        "Select " + COL_A + ", " + COL_B + " from " + TABLE + //
            " order by " + COL_A + ", " + COL_B + " desc LIMIT 2");

    Set<Long> allRowIdsReportedByClusterNodes = new ConcurrentSkipListSet<>();
    // Add a RowId consumer to the ExecuteRemotePlanOnShardsStep in order to collect all rowIDs that have been reported
    // by cluster nodes/remote executions.
    executablePlan.getSteps().stream().filter(step -> step instanceof ExecuteRemotePlanOnShardsStep)
        .forEach(new Consumer<ExecutablePlanStep>() {
          @Override
          public void accept(ExecutablePlanStep executeRemoteStep) {
            executeRemoteStep.addOutputConsumer(new AbstractThreadedRowIdConsumer(null) {
              @Override
              protected void allSourcesAreDone() {
              }

              @Override
              protected synchronized void doConsume(Long[] rowIds) {
                allRowIdsReportedByClusterNodes.addAll(Arrays.asList(rowIds));
              }
            });
          }
        });

    ExecutorService executor = executors.newTestExecutor(executablePlan.preferredExecutorServiceSize());
    try {
      // WHEN
      // executing it on the sample table
      Future<Void> future = executablePlan.executeAsynchronously(executor);
      future.get(); // wait until done.

      // THEN
      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns");

      // we expect a HARD limit to be set, because the remotes can execute the full ordering and can therefore truncate
      // the results accordingly.
      Assert.assertTrue(findRemoteOrderStep(executablePlan).getDetailsOrder().getLimit().isSetLimit(),
          "Expected remote order step to have a hard limit");

      List<Pair<Object, Object>> expectedValues = new ArrayList<>();
      // colA: 1L, colB: 100L
      expectedValues.add(new Pair<>(dp.v(1), dp.v(100)));
      // colA: 2L, colB: 400L
      expectedValues.add(new Pair<>(dp.v(2), dp.v(400)));

      for (int i = 0; i < expectedValues.size(); i++) {
        Pair<Object, Object> expected = expectedValues.get(i);
        long rowId = resultOrderRowIds.get(i);
        Object valueColA = resultValues.get(COL_A).get(rowId);
        Object valueColB = resultValues.get(COL_B).get(rowId);
        Pair<Object, Object> actualResult = new Pair<>(valueColA, valueColB);
        Assert.assertEquals(actualResult, expected,
            "Expected correct result for index " + i + " (rowId " + rowId + ")");
      }

      Assert.assertEquals(resultOrderRowIds.size(), expectedValues.size(), "Expected correct number of result rows");

      // check if correct rowIDs have been reported to query master. We check this by inspecting the values of colA and
      // colB for the rowIds that have been reported and how often those values were reported.
      // Note that we check that _at least_ a certain number of rows with the values was returned. If the timing of the
      // threads is unfortunate on the remotes, the OrderStep might get woken up before all rowIds have been reported to
      // it -> it has a smaller number of rows to order -> it might send rowIds on their journey which would not be part
      // if all the rowIds would have been available to it right away.

      Map<Pair<Object, Object>, Integer> expectedRowIdMinValues = new HashMap<>();
      expectedRowIdMinValues.clear();
      // ColA: 1, ColB: 100, number of times reported: 1
      expectedRowIdMinValues.put(new Pair<>(dp.v(1), dp.v(100)), 1);
      // ColA: 2, ColB: 400, number of times reported: 2
      expectedRowIdMinValues.put(new Pair<>(dp.v(2), dp.v(400)), 2);
      // ColA: 2, ColB: 300, number of times reported: 1
      expectedRowIdMinValues.put(new Pair<>(dp.v(2), dp.v(300)), 1);

      Map<Pair<Object, Object>, Integer> actualRowIdValues = new HashMap<>();
      for (Long reportedRowId : allRowIdsReportedByClusterNodes) {
        Object colAValue = (reportedRowId < colAShard1.length) ? colAShard1[reportedRowId.intValue()]
            : colAShard2[(int) (reportedRowId - colAShard1.length)];
        Object colBValue = (reportedRowId < colBShard1.length) ? colBShard1[reportedRowId.intValue()]
            : colBShard2[(int) (reportedRowId - colBShard1.length)];

        Pair<Object, Object> value = new Pair<>(colAValue, colBValue);
        if (actualRowIdValues.containsKey(value))
          actualRowIdValues.put(value, actualRowIdValues.get(value) + 1);
        else
          actualRowIdValues.put(value, 1);
      }

      Set<Pair<Object, Object>> valuesNotReported =
          Sets.difference(expectedRowIdMinValues.keySet(), actualRowIdValues.keySet());
      Assert.assertTrue(valuesNotReported.isEmpty(),
          "Expected that query master was provided with all interesting row IDs, so that it was "
              + "able to do the ordering correctly. Not reported values: " + valuesNotReported);

      for (Pair<Object, Object> p : expectedRowIdMinValues.keySet())
        Assert.assertTrue(expectedRowIdMinValues.get(p) <= actualRowIdValues.get(p),
            "Expected to receive " + p + "at least " + expectedRowIdMinValues.get(p) + " times, but did receive it "
                + actualRowIdValues.get(p) + " times.");
    } finally {
      executor.shutdownNow();
    }
  }

  private RExecutionPlanStep findRemoteOrderStep(ExecutablePlan plan) {
    for (ExecutablePlanStep masterStep : plan.getSteps()) {
      if (masterStep instanceof ExecuteRemotePlanOnShardsStep) {
        for (RExecutionPlanStep remoteStep : ((ExecuteRemotePlanOnShardsStep) masterStep).getRemoteExecutionPlan()
            .getSteps()) {
          if (remoteStep.getType().equals(RExecutionPlanStepType.ORDER))
            return remoteStep;
        }
      }
    }
    return null;
  }

}
