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
public class DoubleProjectionDiqlExecutionTest extends AbstractCacheDoubleDiqlExecutionTest<Double> {

  public DoubleProjectionDiqlExecutionTest() {
    super(ColumnType.DOUBLE, new DoubleTestDataProvider());
  }

  @Test
  public void simpleProjectionTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select add(" + COL_A + ", 1.) from " + TABLE);
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
          .addParameterColumnName(COL_A).addParameterLiteralDouble(1.).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] + 1.;

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
    ExecutablePlan executablePlan = buildExecutablePlan("Select add(" + COL_A + ", " + COL_B + ") from " + TABLE + //
        " where add(add(1., 1.), " + COL_A + ") = 5.3"); // where -> COL_A == 3.3
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
          .addParameterColumnName(COL_A).addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      int selectedRowId = Arrays.binarySearch(COL_A_DEFAULT_VALUES, 3.3);

      Object[] expectedValues =
          new Double[] { (double) COL_A_DEFAULT_VALUES[selectedRowId] + (double) COL_B_DEFAULT_VALUES[selectedRowId] };

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
    ExecutablePlan executablePlan = buildExecutablePlan("Select add(1., add(500., 1000.)) from " + TABLE);
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
          .addParameterLiteralDouble(1.).addParameterColumnName(functionBasedColumnNameBuilderFactory.create()
              .withFunctionName("add").addParameterLiteralDouble(500.).addParameterLiteralDouble(1000.).build())
          .build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = new Double[] { 1501. };

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected to get result rows for all rows in the input table");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionWithoutColumns2Test() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select id(1.) from " + TABLE);
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

      String resColName =
          functionBasedColumnNameBuilderFactory.create().withFunctionName("id").addParameterLiteralDouble(1.).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = new Double[] { 1. };

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get 1. as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected to get result rows for all rows in the input table");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionOneRowOnlyTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(new Double[] { 1. }, new Double[] { 2. });
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select add(" + COL_A + ", 500.) from " + TABLE);
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
          .addParameterColumnName(COL_A).addParameterLiteralDouble(500.).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(new Double[] { 501. })), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), 1, "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionMulTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select mul(" + COL_A + ", 10.) from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("mul")
          .addParameterColumnName(COL_A).addParameterLiteralDouble(10.).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] * 10.;

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionDivTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select div(" + COL_A + ", 10.) from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("div")
          .addParameterColumnName(COL_A).addParameterLiteralDouble(10.).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] / 10.;

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionSubTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select sub(" + COL_A + ", 10.) from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sub")
          .addParameterColumnName(COL_A).addParameterLiteralDouble(10).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] - 10.;

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionMulTest2() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select mul(" + COL_A + ", " + COL_B + ") from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("mul")
          .addParameterColumnName(COL_A).addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] * (double) COL_B_DEFAULT_VALUES[i];

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionDivTest2() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select div(" + COL_A + ", " + COL_B + ") from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("div")
          .addParameterColumnName(COL_A).addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] / (double) COL_B_DEFAULT_VALUES[i];

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleProjectionSubTest2() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select sub(" + COL_A + ", " + COL_B + ") from " + TABLE);
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

      String resColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sub")
          .addParameterColumnName(COL_A).addParameterColumnName(COL_B).build();

      Assert.assertTrue(resultValues.containsKey(resColName), "Result values should be available for result column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Object[] expectedValues = dp.emptyArray(COL_A_DEFAULT_VALUES.length);
      for (int i = 0; i < expectedValues.length; i++)
        expectedValues[i] = (double) COL_A_DEFAULT_VALUES[i] - (double) COL_B_DEFAULT_VALUES[i];

      Assert.assertEquals(new HashSet<>(resultValues.get(resColName).values()),
          new HashSet<>(Arrays.asList(expectedValues)), "Expected to get sum as result");

      Assert.assertEquals(resultValues.get(resColName).size(), VALUE_LENGTH,
          "Expected specific number of rows in result");
    } finally {
      executor.shutdownNow();
    }
  }
}
