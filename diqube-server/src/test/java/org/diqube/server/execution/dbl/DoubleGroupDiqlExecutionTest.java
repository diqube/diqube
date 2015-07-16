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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.plan.util.FunctionBasedColumnNameBuilder;
import org.diqube.server.execution.GroupDiqlExecutionTest;
import org.diqube.util.DoubleUtil;
import org.diqube.util.Pair;
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
          new FunctionBasedColumnNameBuilder().withFunctionName("avg").addParameterColumnName(COL_B).build();
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
          new FunctionBasedColumnNameBuilder().withFunctionName("avg").addParameterColumnName(COL_B).build();
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

}
