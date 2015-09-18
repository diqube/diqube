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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.LongStream;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.plan.exception.ValidationException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Validates correct execution for simple SELECT/WHERE/ORDER BY statements.
 *
 * @author Bastian Gloeckle
 */
public abstract class SimpleDiqlExecutionTest<T> extends AbstractCacheDoubleDiqlExecutionTest<T> {

  public SimpleDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Test
  public void simpleSelectTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE);
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
      Object[] returnedValuesByRowId = dp.emptyArray(resultValues.get(COL_A).size());
      resultValues.get(COL_A).forEach((k, v) -> returnedValuesByRowId[k.intValue()] = v);
      Assert.assertEquals(returnedValuesByRowId, COL_A_DEFAULT_VALUES,
          "Expected to have all values for col a returned");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void simpleSelectTestOrderOnlyLiterals() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    buildExecutablePlan("Select " + COL_A + " from " + TABLE + " order by add(1, 1)");
  }

  @Test
  public void selectWithRestrictionsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a select stmt with WHERE clause which matches two rows
    int matchedRow1 = 120;
    int matchedRow2 = 100;
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + //
            COL_A + " = " + COL_A_DEFAULT_VALUES_DIQL[matchedRow1] + " and " + COL_B + " = "
            + COL_B_DEFAULT_VALUES_DIQL[matchedRow1] + " or " + COL_A + " = " + COL_A_DEFAULT_VALUES_DIQL[matchedRow2]
            + " and " + COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[matchedRow2] + " or " + COL_A + " = "
            + COL_A_DEFAULT_VALUES_DIQL[150] + " and " + COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[149]);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");
      Map<Object, Object> expected = new HashMap<>();
      expected.put((long) matchedRow1, COL_A_DEFAULT_VALUES[matchedRow1]);
      expected.put((long) matchedRow2, COL_A_DEFAULT_VALUES[matchedRow2]);
      Assert.assertEquals(resultValues.get(COL_A), expected, "Expected to have all values for col a returned");

      expected = new HashMap<>();
      expected.put((long) matchedRow1, COL_B_DEFAULT_VALUES[matchedRow1]);
      expected.put((long) matchedRow2, COL_B_DEFAULT_VALUES[matchedRow2]);
      Assert.assertEquals(resultValues.get(COL_B), expected, "Expected to have all values for col b returned");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleOrderSelectTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + " from " + TABLE + " order by " + COL_B + " desc");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      // we expect reversed order
      Long[] expectedRowIds = new Long[COL_A_DEFAULT_VALUES.length];
      for (int i = 0; i < expectedRowIds.length; i++)
        expectedRowIds[i] = (long) (expectedRowIds.length - 1 - i);

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(expectedRowIds),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleOrderSelectTestWithLimit() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + " from " + TABLE + " order by " + COL_B + " desc limit 5");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      // we expect reversed order
      Object[] expectedRowIds = new Long[5];
      for (int i = 0; i < expectedRowIds.length; i++)
        expectedRowIds[i] = VALUE_LENGTH - 1 - i;

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(expectedRowIds),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void simpleOrderSelectWhereTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    // a simple select stmt
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + //
        " where " + COL_A + " = " + dp.vDiql(5) + " or " + COL_A + " = " + dp.vDiql(4) + " " //
        + "order by " + COL_B + " desc");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Long[] expectedRowIds = new Long[] { 5L, 4L };

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(expectedRowIds),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void complexOrderSelectTest() throws InterruptedException, ExecutionException {
    // GIVEN
    // rowId colA colB
    // (0) 10 8
    // (1) 10 9
    // (2) 10 10
    // (3) 5 1
    // (4) 7 0
    // (5) 1 20
    // -> sort colA ASC, colB DESC
    // expected:
    // (5) 1 20
    // (3) 5 1
    // (4) 7 0
    // (2) 10 10
    // (1) 10 9
    // (0) 10 8
    Object[] colAValues = dp.a(10L, 10L, 10L, 5L, 7L, 1L);
    Object[] colBValues = dp.a(8L, 9L, 10L, 1L, 0L, 20L);
    Long[] orderedRowIdsExpected = new Long[] { 5L, 3L, 4L, 2L, 1L, 0L };
    initializeSimpleTable(colAValues, colBValues);
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + " from " + TABLE + " order by " + COL_A + " asc, " + COL_B + " DESC");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(orderedRowIdsExpected),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void complexOrderSelectTestWithLimit() throws InterruptedException, ExecutionException {
    // GIVEN
    // rowId colA colB
    // (0) 10 8
    // (1) 10 9
    // (2) 10 10
    // (3) 5 1
    // (4) 7 0
    // (5) 1 20
    // -> sort colA ASC, colB DESC
    // expected:
    // (5) 1 20 LIMIT WILL REMOVE THIS ENTRY!
    // (3) 5 1
    // (4) 7 0
    // (2) 10 10
    // (1) 10 9
    // (0) 10 8 LIMIT WILL REMOVE THIS ENTRY!
    Object[] colAValues = dp.a(10L, 10L, 10L, 5L, 7L, 1L);
    Object[] colBValues = dp.a(8L, 9L, 10L, 1L, 0L, 20L);
    Long[] orderedRowIdsExpected = new Long[] { 3L, 4L, 2L, 1L };
    initializeSimpleTable(colAValues, colBValues);
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + " from " + TABLE + " order by " + COL_A + " asc, " + COL_B + " DESC LIMIT 4, 1");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(orderedRowIdsExpected),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void complexOrderSelectTestWithLimitTurnedAround() throws InterruptedException, ExecutionException {
    // GIVEN
    // rowId colA colB
    // (0) 10 8
    // (1) 10 9
    // (2) 10 10
    // (3) 5 1
    // (4) 7 0
    // (5) 1 20
    // -> sort colA DESC, colB ASC
    // expected:
    // (0) 10 8 LIMIT WILL DELETE THIS ENTRY
    // (1) 10 9
    // (2) 10 10
    // (4) 7 0
    // (3) 5 1
    // (5) 1 20 LIMIT WILL DELETE THIS ENTRY
    Object[] colAValues = dp.a(10L, 10L, 10L, 5L, 7L, 1L);
    Object[] colBValues = dp.a(8L, 9L, 10L, 1L, 0L, 20L);
    Long[] orderedRowIdsExpected = new Long[] { 1L, 2L, 4L, 3L };
    initializeSimpleTable(colAValues, colBValues);
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select " + COL_A + " from " + TABLE + " order by " + COL_A + " desc, " + COL_B + " asC LIMIT 4, 1");
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
      Assert.assertNotNull(resultOrderRowIds, "ordered row IDs should have been found");

      Assert.assertTrue(resultValues.containsKey(COL_A), "Result values should be available for column A");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Assert.assertEquals(resultOrderRowIds, Arrays.asList(orderedRowIdsExpected),
          "Expected that the ORDER BY clause reversed the ordering");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(LongStream.range(0L, VALUE_LENGTH).toArray());
    Object[] colBValues = dp.a(LongStream.range(0L, VALUE_LENGTH).map(l -> ((l % 2 == 0) ? l : -1L)).toArray());
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_A + " = " + COL_B);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).size(), VALUE_LENGTH / 2,
          "Expected results for correct number of rows");
      Assert.assertEquals(resultValues.get(COL_B).size(), resultValues.get(COL_A).size(),
          "Expected same number of results for colA and B");

      // check equality for both, rowIds and values.
      Assert.assertEquals(resultValues.get(COL_B), resultValues.get(COL_A),
          "Expected to receive results where colA == colB");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColGtEqRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 1L, 2L, 3L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_A + " >= " + COL_B);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColGtEqRestrictionsTest2() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_B + " >= " + COL_A);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtEqRestrictionsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    int geIndex = (int) (VALUE_LENGTH / 2);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " >= " + COL_A_DEFAULT_VALUES_DIQL[geIndex] + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).size(), VALUE_LENGTH - geIndex,
          "Expected correct number of rows being returned");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = geIndex; i < VALUE_LENGTH; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }

      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected correct results for col A");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected correct results for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtEqNonMatchingRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 4L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " >= " + dp.vDiql(2) + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtEqNonMatchingConstantFirstRestrictionsTest()
      throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 4L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + dp.vDiql(2) + " <= " + COL_A + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColGtRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_A + " > " + COL_B);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColGtRestrictionsTest2() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_B + " > " + COL_A);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtRestrictionsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    int geIndex = (int) (VALUE_LENGTH / 2);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " > " + COL_A_DEFAULT_VALUES_DIQL[geIndex] + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).size(), VALUE_LENGTH - geIndex - 1,
          "Expected correct number of rows being returned");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = geIndex + 1; i < VALUE_LENGTH; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }

      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected correct results for col A");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected correct results for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtNonMatchingRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 4L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " > " + dp.vDiql(2) + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantGtNonMatchingConstantFirstRestrictionsTest()
      throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 4L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + dp.vDiql(2) + " < " + COL_A + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColLtEqRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_A + " <= " + COL_B);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColLtEqRestrictionsTest2() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 1L, 2L, 3L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_B + " <= " + COL_A);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtEqNonMatchingRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " <= " + dp.vDiql(6) + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtEqNonMatchingConstantFirstRestrictionsTest()
      throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where  "
        + dp.vDiql(6) + ">= " + COL_A + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtEqRestrictionsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    int geIndex = (int) (VALUE_LENGTH / 2);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " <= " + COL_A_DEFAULT_VALUES_DIQL[geIndex] + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).size(), VALUE_LENGTH - geIndex + 1,
          "Expected correct number of rows being returned");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = 0; i <= geIndex; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }

      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected correct results for col A");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected correct results for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColLtRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_A + " < " + COL_B);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColColLtRestrictionsTest2() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 2L, 3L, 5L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where " + COL_B + " < " + COL_A);
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtRestrictionsTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    int geIndex = (int) (VALUE_LENGTH / 2);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " < " + COL_A_DEFAULT_VALUES_DIQL[geIndex] + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).size(), VALUE_LENGTH - geIndex,
          "Expected correct number of rows being returned");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = 0; i < geIndex; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }

      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected correct results for col A");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected correct results for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtNonMatchingRestrictionsTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where "
        + COL_A + " < " + dp.vDiql(6) + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithColConstantLtNonMatchingConstantFirstRestrictionsTest()
      throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(-1L, 1L, 3L, 4L, 5L, 15L);
    Object[] colBValues = dp.a(0L, 1L, 2L, 3L, 10L, 11L);
    Set<Long> expectedResultRowIds = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L }));
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where  "
        + dp.vDiql(6) + "> " + COL_A + " order by " + COL_A + " asc");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Assert.assertEquals(resultValues.get(COL_A).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col A");
      Assert.assertEquals(resultValues.get(COL_B).keySet(), expectedResultRowIds,
          "Expected results for correct rowIds for col B");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithRestrictionsWithNotTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    int unmatchedRow1 = 10;
    int unmatchedRow2 = (int) (VALUE_LENGTH - 1);
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE
        + " where not (" + //
        COL_A + " = " + COL_A_DEFAULT_VALUES_DIQL[unmatchedRow1] + " and " + COL_B + " = "
        + COL_B_DEFAULT_VALUES_DIQL[unmatchedRow1] + " or " + COL_A + " = " + COL_A_DEFAULT_VALUES_DIQL[unmatchedRow2]
        + " and " + COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[unmatchedRow2] + " or " + COL_A + " = "
        + COL_A_DEFAULT_VALUES_DIQL[150] + " and " + COL_B + " = " + COL_B_DEFAULT_VALUES_DIQL[149] + //
        ")");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = 0; i < VALUE_LENGTH; i++) {
        if (i == unmatchedRow1 || i == unmatchedRow2)
          continue;
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }
      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected to have all values for col a returned");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected to have all values for col b returned");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectWithRestrictionsWithNotInequalTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    int firstUnmatchedRow = 10;
    int lastUnmatchedRow = (int) (VALUE_LENGTH - 2);
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + COL_A + ", " + COL_B + " from " + TABLE + " where not (" //
            + COL_A + " >= " + COL_A_DEFAULT_VALUES_DIQL[firstUnmatchedRow] + " and " + COL_B + " <= "
            + COL_B_DEFAULT_VALUES_DIQL[lastUnmatchedRow] + ")");
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
      Assert.assertTrue(resultValues.containsKey(COL_B), "Result values should be available for column B");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Map<Long, Object> expectedColA = new HashMap<>();
      Map<Long, Object> expectedColB = new HashMap<>();
      for (int i = 0; i < firstUnmatchedRow; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }
      for (int i = lastUnmatchedRow + 1; i < VALUE_LENGTH; i++) {
        expectedColA.put((long) i, COL_A_DEFAULT_VALUES[i]);
        expectedColB.put((long) i, COL_B_DEFAULT_VALUES[i]);
      }
      Assert.assertEquals(resultValues.get(COL_A), expectedColA, "Expected to have all values for col a returned");
      Assert.assertEquals(resultValues.get(COL_B), expectedColB, "Expected to have all values for col b returned");
    } finally {
      executor.shutdownNow();
    }
  }

}
