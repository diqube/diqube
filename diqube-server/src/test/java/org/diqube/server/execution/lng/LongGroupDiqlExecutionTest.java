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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.plan.exception.ValidationException;
import org.diqube.server.execution.GroupDiqlExecutionTest;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
@Test
public class LongGroupDiqlExecutionTest extends GroupDiqlExecutionTest<Long> {

  public LongGroupDiqlExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @Test
  public void simpleGroupAggregationProjection1Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", add(count(), 1) from " + TABLE + " where " + COL_A + " = 1 group by " + COL_A);
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column");
      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build())
          .addParameterLiteralLong(1L).build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for aggregated res column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Object[] expectedValues = dp.a(1);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to get value 1L as result");

      expectedValues = dp.a(4);
      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get value 4L as result for aggregated col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupAggregationProjection2Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt, aggregation on projected column, this happens only for resolving fields (not in where etc).
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", avg(add(" + COL_A + ", 1)) from " + TABLE
        + " where " + COL_A + " = " + dp.vDiql(1) + " group by " + COL_A);
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column a");

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
              .addParameterColumnName(COL_A).addParameterLiteralLong(1L).build())
          .build();

      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for result count column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Object[] expectedValues = dp.a(1);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to get value 1L as result for colA");

      expectedValues = new Double[] { 2. }; // avg(add(colA, 1)) with colA == 1.
      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get value 2. as result for count col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupAggregationProjection3Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", add(avg(add(" + COL_A
        + ", 1)), 1.) from " + TABLE + " where " + COL_A + " = 1 group by " + COL_A);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
                  .addParameterColumnName(COL_A).addParameterLiteralLong(1L).build())
              .build())
          .addParameterLiteralDouble(1.).build();

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column");
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for result count column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Object[] expectedValues = dp.a(1);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to get value 1L as result");

      expectedValues = new Double[] { 3. }; // add(avg(add(colA, 1)), 1.) with colA == 1.
      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get correct value as result for count col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupAggregationOnProjectionTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", add(count(), 1) from " + TABLE + " where add(" + COL_A + ", 1) = 2 group by " + COL_A);
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column");
      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build())
          .addParameterLiteralLong(1L).build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for aggregated res column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Object[] expectedValues = dp.a(1);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to get value 1L as result");

      expectedValues = dp.a(4);
      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get value 4L as result for aggregated col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupOrderingTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(99, 5, 100, 99, 1, 99);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", count() from " + TABLE + //
        " group by " + COL_A + //
        " order by add(count(), 1) desc, " + COL_A // -> order by count, then by colA
    );
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column A");
      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for result aggregated col");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      List<Pair<Object, Object>> expectedResult = new ArrayList<>();
      // ColA: 99L, count: 3L
      expectedResult.add(new Pair<>(dp.v(99), dp.v(3)));
      // ColA: 1L, count: 1L
      expectedResult.add(new Pair<>(dp.v(1), dp.v(1)));
      // ColA: 5L, count: 1L
      expectedResult.add(new Pair<>(dp.v(5), dp.v(1)));
      // ColA: 100L, count: 1L
      expectedResult.add(new Pair<>(dp.v(100), dp.v(1)));

      for (int orderedRowId = 0; orderedRowId < expectedResult.size(); orderedRowId++) {
        long rowId = resultOrderRowIds.get(orderedRowId);
        Object colAValue = resultValues.get(COL_A).get(rowId);
        Object countValue = resultValues.get(resColName).get(rowId);

        Pair<Object, Object> actualValue = new Pair<>(colAValue, countValue);

        Assert.assertEquals(actualValue, expectedResult.get(orderedRowId),
            "Expected correct result at ordered index " + orderedRowId + " (" + rowId + ")");
      }

      Assert.assertEquals(resultOrderRowIds.size(), expectedResult.size(),
          "Expected to receive correct number of rows");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoAggregationsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 2, 1, 2, 1, 2);
    Object[] colBValues = dp.a(10, 20, 10, 20, 10, 20);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", count(), round(avg(" + COL_B + ")) from " + TABLE + //
            " group by " + COL_A + //
            " order by avg(" + COL_B + ") desc");
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column A");
      String resCountColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();
      String resAvgColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(COL_B).build())
          .build();
      Assert.assertTrue(resultValues.containsKey(resCountColName),
          "Result values should be available for result count col");
      Assert.assertTrue(resultValues.containsKey(resAvgColName),
          "Result values should be available for result avg col");
      Assert.assertEquals(resultValues.size(), 3, "Result values should be available for specific amount of cols only");

      List<Triple<Object, Long, Long>> expectedResult = new ArrayList<>();
      // ColA: 2L, count: 3L, avg: 20L
      expectedResult.add(new Triple<>(dp.v(2), 3L, 20L));
      // ColA: 1L, count: 3L, avg: 10L
      expectedResult.add(new Triple<>(dp.v(1), 3L, 10L));

      for (int orderedRowId = 0; orderedRowId < expectedResult.size(); orderedRowId++) {
        long rowId = resultOrderRowIds.get(orderedRowId);
        Object colAValue = resultValues.get(COL_A).get(rowId);
        Long countValue = resultValues.get(resCountColName).get(rowId);
        Long avgValue = resultValues.get(resAvgColName).get(rowId);

        Triple<Object, Long, Long> actualValue = new Triple<>(colAValue, countValue, avgValue);

        Assert.assertEquals(actualValue, expectedResult.get(orderedRowId),
            "Expected correct result at ordered index " + orderedRowId + " (" + rowId + ")");
      }

      Assert.assertEquals(resultOrderRowIds.size(), expectedResult.size(),
          "Expected to receive correct number of rows");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void emptyAggregation1Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", count() from " + TABLE + " where " + COL_A + " > 100 group by " + COL_A);
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

      Assert.assertTrue(resultValues.isEmpty(), "Expected no results.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void emptyAggregation2Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", add(count(), 1) from " + TABLE + " where " + COL_A + " > 100 group by " + COL_A);
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

      Assert.assertTrue(resultValues.isEmpty(), "Expected no results.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void emptyAggregation3Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + " from " + TABLE + " where " + COL_A + " > 100 group by " + COL_A + " order by count()");
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

      Assert.assertTrue(resultValues.isEmpty(), "Expected no results.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void groupOnProjection1Test() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(97, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(2, 1, 0, 5, 0, 5);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select add(" + COL_B + ", " + COL_A + "), count() from "
        + TABLE + " group by add(" + COL_A + ", " + COL_B + ") order by count() desc");
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

      String resAddColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(COL_B).addParameterColumnName(COL_A).build();
      String resCountColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();

      Assert.assertTrue(resultValues.containsKey(resAddColName), "Expected results for add col");
      Assert.assertTrue(resultValues.containsKey(resCountColName), "Expected results for count col");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have correct amount of result cols");

      List<Pair<Long, Long>> expectedResult = new ArrayList<>();
      expectedResult.add(new Pair<>(6L, 3L));
      expectedResult.add(new Pair<>(99L, 2L));
      expectedResult.add(new Pair<>(100L, 1L));

      for (int orderedRowId = 0; orderedRowId < expectedResult.size(); orderedRowId++) {
        long rowId = resultOrderRowIds.get(orderedRowId);
        Long addValue = resultValues.get(resAddColName).get(rowId);
        Long countValue = resultValues.get(resCountColName).get(rowId);

        Pair<Long, Long> actualValue = new Pair<>(addValue, countValue);

        Assert.assertEquals(actualValue, expectedResult.get(orderedRowId),
            "Expected correct result at ordered index " + orderedRowId + " (" + rowId + ")");
      }

    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void groupOnAggregationTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(97, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(2, 1, 0, 5, 0, 5);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN WHEN
    buildExecutablePlan("Select " + COL_A + ", count() from " + TABLE + " group by add(" + COL_A + ", count())");
    // THEN: exception.
  }

  @Test(expectedExceptions = ValidationException.class)
  public void groupOnContantTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(97, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(2, 1, 0, 5, 0, 5);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN WHEN
    buildExecutablePlan("Select " + COL_A + ", count() from " + TABLE + " group by id(1)");
    // THEN: exception.
  }

  @Test
  public void aggregationFunctionWithConstantParam() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(3, 0, 0, 2, 0, 10);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + " from " + TABLE + " group by " + COL_A + " having any(10, " + COL_B + ") = 1");
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

      Assert.assertEquals(resultHavingRowIds.length, 1, "Expected results for columns.");
      Assert.assertNotNull(resultValues.get(COL_A), "Expected results for col A.");

      Assert.assertEquals((long) resultValues.get(COL_A).get(resultHavingRowIds[0]), 1L, "Expected correct value.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void minMax() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(3, 1, 50, 2, 0, 10);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", max(" + COL_B + "), min(" + COL_B + ") from " + TABLE + " group by " + COL_A);
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

      String resMaxColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("max").addParameterColumnName(COL_B).build();
      String resMinColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("min").addParameterColumnName(COL_B).build();

      Assert.assertNotNull(resultValues.get(COL_A), "Col A expected to be abailable");
      Assert.assertNotNull(resultValues.get(resMinColName), "Min col expected to be abailable");
      Assert.assertNotNull(resultValues.get(resMaxColName), "Max col expected to be abailable");

      Set<Triple<Long, Long, Long>> expected = new HashSet<>();
      expected.add(new Triple<>(1L, 10L, 2L));
      expected.add(new Triple<>(5L, 1L, 1L));
      expected.add(new Triple<>(100L, 50L, 50L));
      expected.add(new Triple<>(99L, 0L, 0L));

      Set<Triple<Long, Long, Long>> actual = new HashSet<>();
      for (long rowId : resultValues.get(COL_A).keySet()) {
        actual.add(new Triple<>(resultValues.get(COL_A).get(rowId), resultValues.get(resMaxColName).get(rowId),
            resultValues.get(resMinColName).get(rowId)));
      }

      Assert.assertEquals(actual, expected, "Expected correct values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void overflowAvgTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 1, 5, 99, 1);
    Object[] colBValues = dp.a(Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE, 0, Long.MAX_VALUE);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", round(avg(" + COL_B + ")) from " + TABLE + " group by " + COL_A);
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column a");

      String resAvgColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(COL_B).build())
          .build();

      Assert.assertTrue(resultValues.containsKey(resAvgColName),
          "Result values should be available for result avg column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, Long.MAX_VALUE));
      expected.add(new Pair<>(5L, (Long.MAX_VALUE - Long.MIN_VALUE) / 2));
      expected.add(new Pair<>(99L, 0L));

      Set<Pair<Long, Long>> actual = new HashSet<>();
      for (long rowId : resultValues.get(COL_A).keySet())
        actual.add(new Pair<>(resultValues.get(COL_A).get(rowId), resultValues.get(resAvgColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct values.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void sumTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 1, 5, 99, 1);
    Object[] colBValues = dp.a(1, 5, 2, 6, -500, 3);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", sum(" + COL_B + ") from " + TABLE + " group by " + COL_A);
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column a");

      String resAvgColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("sum").addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resAvgColName),
          "Result values should be available for result avg column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 1L + 2L + 3L));
      expected.add(new Pair<>(5L, 5L + 6L));
      expected.add(new Pair<>(99L, -500L));

      Set<Pair<Long, Long>> actual = new HashSet<>();
      for (long rowId : resultValues.get(COL_A).keySet())
        actual.add(new Pair<>(resultValues.get(COL_A).get(rowId), resultValues.get(resAvgColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct values.");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void groupAggregationProjectionWithOrderLimitTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 5, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", add(count(), 1) from " + TABLE
        + " group by " + COL_A + " order by add(count(), 1) desc limit 3");
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

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column");
      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build())
          .addParameterLiteralLong(1L).build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for aggregated res column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      List<Pair<Long, Long>> expectedValues = new ArrayList<>();
      expectedValues.add(new Pair<>(1L, 4L));
      expectedValues.add(new Pair<>(5L, 3L));
      expectedValues.add(new Pair<>(99L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (long rowId : resultOrderRowIds)
        actual.add(new Pair<>(resultValues.get(COL_A).get(rowId), resultValues.get(resColName).get(rowId)));

      Assert.assertEquals(actual, expectedValues, "Expected to get correct results");
    } finally {
      executor.shutdownNow();
    }
  }

}
