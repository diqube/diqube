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
import java.util.stream.Collectors;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.function.projection.IdLongFunction;
import org.diqube.loader.LoadException;
import org.diqube.server.execution.SimpleDiqlExecutionTest;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
@Test
public class LongSimpleDiqlExecutionTest extends SimpleDiqlExecutionTest<Long> {

  public LongSimpleDiqlExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @Test
  public void selectEmptyTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " where " + COL_B + " > "
        + COL_B_DEFAULT_VALUES[COL_B_DEFAULT_VALUES.length - 1] + " order by " + COL_A + " desc");
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

      Assert.assertTrue(resultValues.isEmpty(), "Did not expect to have a result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectEmptyProjectedColTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " where " + COL_B + " > "
        + COL_B_DEFAULT_VALUES[COL_B_DEFAULT_VALUES.length - 1] + " order by add(" + COL_A + ", 1) desc");
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

      Assert.assertTrue(resultValues.isEmpty(), "Did not expect to have a result");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void negativeValueInDiqlTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select add(" + COL_A + ", -1) from " + TABLE + " where " + COL_B + " > -1 and " + COL_A + " < 100");
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

      String outColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
          .addParameterColumnName(COL_A).addParameterLiteralLong(-1L).build();

      Assert.assertTrue(resultValues.containsKey(outColName), "Result values should be available for output column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Set<Long> expected = new HashSet<>();
      for (int i = 0; i < 100; i++)
        expected.add((long) (i - 1));

      Set<Long> actual = resultValues.get(outColName).values().stream().collect(Collectors.toSet());

      Assert.assertEquals(actual, expected, "Expected to have correct results");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void columnSameNameAsFunctionTest() throws InterruptedException, ExecutionException, LoadException {
    String colName = IdLongFunction.NAME;
    initializeFromJson("[{\"" + colName + "\" : 1}]");
    // GIVEN
    ExecutablePlan executablePlan =
        buildExecutablePlan("Select " + colName + ", " + IdLongFunction.NAME + "(" + colName + ") from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(executablePlan.preferredExecutorServiceSize());
    try {
      // WHEN
      // executing it on the sample table
      Future<Void> future = executablePlan.executeAsynchronously(executor);
      future.get(); // wait until done.

      String secondColName = functionBasedColumnNameBuilderFactory.create().withFunctionName(IdLongFunction.NAME)
          .addParameterColumnName(colName).build();

      // THEN
      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey(colName), "Result values should be available for output column");
      Assert.assertTrue(resultValues.containsKey(secondColName),
          "Result values should be available for second output column");
      Assert.assertEquals(resultValues.size(), 2, "Result values should be available for two columns only");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      for (int i = 0; i < 100; i++)
        expected.add(new Pair<>(1L, 1L));

      Set<Pair<Long, Long>> actual = new HashSet<>();
      for (long rowId : resultValues.get(colName).keySet())
        actual.add(new Pair<>(resultValues.get(colName).get(rowId), resultValues.get(secondColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected to have correct results");
    } finally {
      executor.shutdownNow();
    }
  }

}
