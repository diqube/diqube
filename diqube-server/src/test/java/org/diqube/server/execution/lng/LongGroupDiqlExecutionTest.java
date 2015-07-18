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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.plan.util.FunctionBasedColumnNameBuilder;
import org.diqube.server.execution.GroupDiqlExecutionTest;
import org.diqube.util.Pair;
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
      String resColName = new FunctionBasedColumnNameBuilder().withFunctionName("add")
          .addParameterColumnName(new FunctionBasedColumnNameBuilder().withFunctionName("count").build())
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

      String resColName = new FunctionBasedColumnNameBuilder().withFunctionName("avg")
          .addParameterColumnName(new FunctionBasedColumnNameBuilder().withFunctionName("add")
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

      String resColName = new FunctionBasedColumnNameBuilder().withFunctionName("add")
          .addParameterColumnName(new FunctionBasedColumnNameBuilder().withFunctionName("avg")
              .addParameterColumnName(new FunctionBasedColumnNameBuilder().withFunctionName("add")
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
      String resColName = new FunctionBasedColumnNameBuilder().withFunctionName("add")
          .addParameterColumnName(new FunctionBasedColumnNameBuilder().withFunctionName("count").build())
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
      String resColName = new FunctionBasedColumnNameBuilder().withFunctionName("count").build();
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

}