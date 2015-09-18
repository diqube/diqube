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

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Validates result of diql queries with GROUP BY.
 *
 * @author Bastian Gloeckle
 */
public abstract class GroupDiqlExecutionTest<T> extends AbstractCacheDoubleDiqlExecutionTest<T> {

  public GroupDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Test
  public void simpleSelectGroupTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.emptyArray((int) VALUE_LENGTH);
    for (int i = 0; i < colAValues.length; i++)
      colAValues[i] = dp.v(i % 99);
    initializeSimpleTable(colAValues, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " group by " + COL_A);
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
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(99);
      for (int i = 0; i < 99; i++)
        expectedValues[i] = dp.v(i);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to receive values of 99 groups");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleSelectGroupWhereTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.emptyArray((int) VALUE_LENGTH);
    int lastIndexWithValueZero = -1;
    for (int i = 0; i < colAValues.length; i++) {
      colAValues[i] = dp.v(i % 99);
      if (i % 99 == 0L)
        lastIndexWithValueZero = i;
    }
    initializeSimpleTable(colAValues, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " where " + //
        COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[0] + " or " + //
        COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[1] + " or " + //
        COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[lastIndexWithValueZero] + //
        " group by " + COL_A);
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
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.a(0, 1); // for the first group value 0L, for the second group
      // value 1L.

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to receive values of 2 groups");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleGroupAggregationTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", count() from " + TABLE + " where " + COL_A + " = " + dp.vDiql(1) + " group by " + COL_A);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for result column a");
      Assert.assertTrue(resultValues.containsKey(resColName),
          "Result values should be available for result count column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for one column only");

      Object[] expectedValues = dp.a(1);

      Assert.assertEquals(new HashSet<>(resultValues.get(COL_A).values()), new HashSet<>(Arrays.asList(expectedValues)),
          "Expected to get value 1L as result for colA");

      expectedValues = new Long[] { 3L };
      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get value 3L as result for count col");
    } finally {
      executor.shutdownNow();
    }
  }

}
