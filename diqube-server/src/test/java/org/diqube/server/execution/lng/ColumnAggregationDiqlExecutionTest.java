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
package org.diqube.server.execution.lng;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.loader.LoadException;
import org.diqube.plan.util.FunctionBasedColumnNameBuilder;
import org.diqube.server.execution.AbstractDiqlExecutionTest;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test the column aggregation features on LONG columns.
 * 
 * The column aggregation aggregates the values of multiple columns of the same row to one new value. This is used if an
 * aggregation is executed on repeated fields for example. In contrast to that, aggregation that is executed on a GROUP
 * BY does aggregate values of the same column but multiple rows.
 *
 * @author Bastian Gloeckle
 */
public class ColumnAggregationDiqlExecutionTest extends AbstractDiqlExecutionTest<Long> {
  public ColumnAggregationDiqlExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @Test
  public void simpleColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson("[ { \"a\": 1, \"b\": [ { \"c\": 0 }, { \"c\": 10 } ] } ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = new FunctionBasedColumnNameBuilder().withFunctionName("round")
          .addParameterColumnName(
              new FunctionBasedColumnNameBuilder().withFunctionName("avg").addParameterColumnName("b[*].c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 1, "Expected to receive a specific amout of rows");

      Assert.assertEquals((long) resultValues.get("a").values().iterator().next(), 1L, "Expected correct result value");
      Assert.assertEquals((long) resultValues.get(resAggColName).values().iterator().next(), 5L,
          "Expected correct result value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": 0 }, { \"c\": 10 } ] }," + //
            "{ \"a\": 2, \"b\": [ { \"c\": 0 }, { \"c\": 100 } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = new FunctionBasedColumnNameBuilder().withFunctionName("round")
          .addParameterColumnName(
              new FunctionBasedColumnNameBuilder().withFunctionName("avg").addParameterColumnName("b[*].c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 5L));
      expected.add(new Pair<>(2L, 50L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowTwoLevelColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ { \"d\": 0 }, { \"d\": 20 } ] }, { \"c\": [ { \"d\": 100 }, { \"d\": 80 } ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ { \"d\": 0 }, { \"d\": 200 } ] }, { \"c\": [ { \"d\": 1000 }, { \"d\": 800 } ] } ] }"
            + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c[*].d)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = new FunctionBasedColumnNameBuilder().withFunctionName("round").addParameterColumnName(
          new FunctionBasedColumnNameBuilder().withFunctionName("avg").addParameterColumnName("b[*].c[*].d").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 50L));
      expected.add(new Pair<>(2L, 500L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

}
