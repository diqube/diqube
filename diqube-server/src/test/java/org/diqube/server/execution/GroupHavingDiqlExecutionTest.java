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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test testing execution of grouped queries that have a HAVING statement.
 *
 * @author Bastian Gloeckle
 */
public abstract class GroupHavingDiqlExecutionTest<T> extends AbstractCacheDoubleDiqlExecutionTest<T> {
  public GroupHavingDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Test
  public void simpleGroupHavingTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", count() from " + TABLE + " group by " + COL_A + " having count() = 3");
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
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertNotNull(resultHavingRowIds, "Expected to have rowIds calculated from the HAVING clause");
      Assert.assertEquals(resultHavingRowIds.length, 1, "Expected specific rows to match HAVING clause");

      Assert.assertEquals(resultValues.get(COL_A).get(resultHavingRowIds[0]), dp.v(1),
          "Expected to get correct value for colA");

      Assert.assertEquals(resultValues.get(resColName).get(resultHavingRowIds[0]), 3L,
          "Expected to get correct value for aggregated col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void inequalGroupHavingTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(0, 0, 0, 0, 0, 0);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + ", count() from " + TABLE + " group by " + COL_A + " having count() > 2");
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
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertNotNull(resultHavingRowIds, "Expected to have rowIds calculated from the HAVING clause");
      Assert.assertEquals(resultHavingRowIds.length, 1, "Expected specific rows to match HAVING clause");

      Assert.assertEquals(resultValues.get(COL_A).get(resultHavingRowIds[0]), dp.v(1),
          "Expected to get correct value for colA");

      Assert.assertEquals(resultValues.get(resColName).get(resultHavingRowIds[0]), 3L,
          "Expected to get correct value for aggregated col");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void inequalGroupHavingAndTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 2, 2, 1, 2, 1);
    Object[] colBValues = dp.a(1, 5, 5, 1, 5, 1);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", count() from " + TABLE + " group by "
        + COL_A + " having count() > 2 and avg(" + COL_B + ") >= 5.");
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
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertNotNull(resultHavingRowIds, "Expected to have rowIds calculated from the HAVING clause");
      Assert.assertEquals(resultHavingRowIds.length, 1, "Expected specific rows to match HAVING clause");

      Assert.assertEquals(resultValues.get(COL_A).get(resultHavingRowIds[0]), dp.v(2),
          "Expected to get correct value for colA");

      Assert.assertEquals(resultValues.get(resColName).get(resultHavingRowIds[0]), 3L,
          "Expected to get correct value for aggregated col");
    } finally {
      executor.shutdownNow();
    }
  }
}
