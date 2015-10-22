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
package org.diqube.server.execution.dbl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.column.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.server.execution.GroupDiqlExecutionTest;
import org.diqube.util.DoubleUtil;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
@Test
public class DoubleGroupDiqlExecutionTest extends GroupDiqlExecutionTest<Double> {

  public DoubleGroupDiqlExecutionTest() {
    super(ColumnType.DOUBLE, new DoubleTestDataProvider());
  }

  @Test
  public void simpleGroupAggregationAvgTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1, 100);
    Object[] colBValues = new Double[] { 0., 4.5, 5.1, 2., 100.01, 2., 5.3 };
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", avg(" + COL_B + ") from " + TABLE + " group by " + COL_A);
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
      String resColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("avg").addParameterColumnName(COL_B).build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for aggregated res column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for one column only");

      Map<Double, Double> expected = new HashMap<>();
      expected.put(dp.v(1), (0. + 2. + 2.) / 3);
      expected.put(dp.v(5), 4.5);
      expected.put(dp.v(100), (5.1 + 5.3) / 2);
      expected.put(dp.v(99), 100.01);

      Map<Double, Double> actual = new HashMap<>();
      for (Entry<Long, Double> colAEntry : resultValues.get(COL_A).entrySet())
        actual.put(colAEntry.getValue(), resultValues.get(resColName).get(colAEntry.getKey()));

      for (Entry<Double, Double> expectedEntry : expected.entrySet()) {
        if (!DoubleUtil.equals(expectedEntry.getValue(), actual.get(expectedEntry.getKey())))
          Assert.fail("Incorrect aggregation result for key " + expectedEntry.getKey() + ": Expected "
              + expectedEntry.getValue() + " but was " + actual.get(expectedEntry.getKey()));
      }

      Assert.assertEquals(actual.size(), expected.size(), "Not correct number of results available");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupOrderingTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1, 100);
    Object[] colBValues = new Double[] { 0., 4.5, 5.1, 2., 100.01, 2., 5.3 };
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", avg(" + COL_B + ") from " + TABLE + //
        " group by " + COL_A + //
        " order by avg(" + COL_B + ") desc" // -> order by avg
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
      String resColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("avg").addParameterColumnName(COL_B).build();
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for result aggregated col");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      List<Pair<Double, Double>> expectedResult = new ArrayList<>();
      // ColA: v(99), avg: 100.01
      expectedResult.add(new Pair<>(dp.v(99), 100.01));
      // ColA: v(100), avg: 5.2
      expectedResult.add(new Pair<>(dp.v(100), (5.1 + 5.3) / 2));
      // ColA: v(5), avg: 4.5
      expectedResult.add(new Pair<>(dp.v(5), 4.5));
      // ColA: v(1), avg: 4/3
      expectedResult.add(new Pair<>(dp.v(1), (0. + 2. + 2.) / 3));

      for (int orderedRowId = 0; orderedRowId < expectedResult.size(); orderedRowId++) {
        long rowId = resultOrderRowIds.get(orderedRowId);
        Double colAValue = resultValues.get(COL_A).get(rowId);
        Double countValue = resultValues.get(resColName).get(rowId);

        Pair<Double, Double> actualValue = new Pair<>(colAValue, countValue);

        if (!DoubleUtil.equals(expectedResult.get(orderedRowId).getLeft(), actualValue.getLeft())
            || !DoubleUtil.equals(expectedResult.get(orderedRowId).getRight(), actualValue.getRight()))
          Assert.fail("Expected correct result at ordered index " + orderedRowId + " (" + rowId + "): "
              + expectedResult.get(orderedRowId).getLeft() + "-" + expectedResult.get(orderedRowId).getRight()
              + " but was " + actualValue.getLeft() + "-" + actualValue.getRight());
      }

      Assert.assertEquals(resultOrderRowIds.size(), expectedResult.size(),
          "Expected to receive correct number of rows");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void aggregationFunctionWithConstantParam() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(3, 0, 0, 2, 0, 10);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " group by " + COL_A
        + " having any(" + dp.vDiql(10) + ", " + COL_B + ") = 1");
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

      Assert.assertEquals((double) resultValues.get(COL_A).get(resultHavingRowIds[0]), dp.v(1),
          "Expected correct value.");
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

      Set<Triple<Double, Double, Double>> expected = new HashSet<>();
      expected.add(new Triple<>(dp.v(1), dp.v(10), dp.v(2)));
      expected.add(new Triple<>(dp.v(5), dp.v(1), dp.v(1)));
      expected.add(new Triple<>(dp.v(100), dp.v(50), dp.v(50)));
      expected.add(new Triple<>(dp.v(99), dp.v(0), dp.v(0)));

      Set<Triple<Double, Double, Double>> actual = new HashSet<>();
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
    Object[] colBValues = new Double[] { Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE - 1, Double.MIN_VALUE, 0.,
        Double.MAX_VALUE };
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", avg(" + COL_B + ") from " + TABLE + " group by " + COL_A);
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
          functionBasedColumnNameBuilderFactory.create().withFunctionName("avg").addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resAvgColName),
          "Result values should be available for result avg column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      // calculate result for dp.v(1) exactly the same way as the unction calculates it - to get the same inaccurancies
      // in rounding etc.
      Double resultOne = Double.MAX_VALUE;
      Double resultTwo = resultOne / 2. + ((Double.MAX_VALUE - 1) / 2.);
      Double resultThree = resultTwo * (2. / 3.) + (Double.MAX_VALUE / 3.);

      Map<Double, Double> expected = new HashMap<>();
      expected.put(dp.v(1), resultThree);
      expected.put(dp.v(5), (Double.MAX_VALUE - Double.MIN_VALUE) / 2.);
      expected.put(dp.v(99), 0.);

      for (long rowId : resultValues.get(COL_A).keySet()) {
        Double colA = resultValues.get(COL_A).get(rowId);
        Double avg = resultValues.get(resAvgColName).get(rowId);
        Assert.assertTrue(DoubleUtil.equals(expected.get(colA), avg),
            "Expected correct value for colA " + colA + ". Expected " + expected.get(colA) + " but was " + avg);
      }
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

      Set<Pair<Double, Double>> expected = new HashSet<>();
      expected.add(new Pair<>(dp.v(1), dp.v(1) + dp.v(2) + dp.v(3)));
      expected.add(new Pair<>(dp.v(5), dp.v(5) + dp.v(6)));
      expected.add(new Pair<>(dp.v(99), dp.v(-500)));

      Set<Pair<Double, Double>> actual = new HashSet<>();
      for (long rowId : resultValues.get(COL_A).keySet())
        actual.add(new Pair<>(resultValues.get(COL_A).get(rowId), resultValues.get(resAvgColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct values.");
    } finally {
      executor.shutdownNow();
    }
  }
}
