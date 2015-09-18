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
package org.diqube.server.execution.str;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.server.execution.AbstractCacheDoubleDiqlExecutionTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Validates results of diql queries with projections.
 *
 * @author Bastian Gloeckle
 */
@Test
public class StringProjectionDiqlExecutionTest extends AbstractCacheDoubleDiqlExecutionTest<String> {
  public StringProjectionDiqlExecutionTest() {
    super(ColumnType.STRING, new StringTestDataProvider());
  }

  @Test
  public void simpleProjectionTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select concat(" + COL_A + ", 'a') from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("concat")
          .addParameterColumnName(COL_A).addParameterLiteralString("a").build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = COL_A_DEFAULT_VALUES[i] + "a";

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionParamsSwitchedTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select concat('a', " + COL_A + ") from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("concat")
          .addParameterLiteralString("a").addParameterColumnName(COL_A).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = "a" + COL_A_DEFAULT_VALUES[i];

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void complexProjectionTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select concat(" + COL_A + ", " + COL_B + ") from " + TABLE + //
        " where concat(concat('a', 'b'), " + COL_A + ") = 'ab" + dp.v(3) + "'"); // where -> COL_A == 3
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("concat")
          .addParameterColumnName(COL_A).addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      int selectedRowId = Arrays.binarySearch(COL_A_DEFAULT_VALUES, dp.v(3));

      Object[] expectedValues =
          new String[] { (String) COL_A_DEFAULT_VALUES[selectedRowId] + COL_B_DEFAULT_VALUES[selectedRowId] };

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), 1, "Expected one one row in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionWithoutColumnsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select concat('a', concat('b', 'c')) from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("concat")
          .addParameterLiteralString("a").addParameterColumnName(functionBasedColumnNameBuilderFactory.create()
              .withFunctionName("concat").addParameterLiteralString("b").addParameterLiteralString("c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = new String[] { "abc" };

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected to get result rows for all rows in the input table");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionOneRowOnlyTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(new String[] { "x" }, new String[] { "y" });
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select concat(" + COL_A + ", 'a') from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("concat")
          .addParameterColumnName(COL_A).addParameterLiteralString("a").build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(new String[] { "xa" })), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), 1, "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

}
