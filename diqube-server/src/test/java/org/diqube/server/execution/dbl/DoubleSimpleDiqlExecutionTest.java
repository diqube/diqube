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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.server.execution.SimpleDiqlExecutionTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
@Test
public class DoubleSimpleDiqlExecutionTest extends SimpleDiqlExecutionTest<Double> {

  public DoubleSimpleDiqlExecutionTest() {
    super(ColumnType.DOUBLE, new DoubleTestDataProvider());
  }

  @Test
  public void negativeValueInDiqlTest() throws InterruptedException, ExecutionException {
    initializeSimpleTable(COL_A_DEFAULT_VALUES, COL_B_DEFAULT_VALUES);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan(
        "Select add(" + COL_A + ", -1.) from " + TABLE + " where " + COL_B + " > -.5 and " + COL_A + " < 100.0");
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
          .addParameterColumnName(COL_A).addParameterLiteralDouble(-1.).build();

      Assert.assertTrue(resultValues.containsKey(outColName), "Result values should be available for output column");
      Assert.assertEquals(resultValues.size(), 1, "Result values should be available for one column only");

      Set<Object> expected = new HashSet<>();
      for (long i = 0; dp.v(i) < 100.0; i++)
        expected.add(dp.v(i) - 1.);

      Set<Double> actual = resultValues.get(outColName).values().stream().collect(Collectors.toSet());

      Assert.assertEquals(actual, expected, "Expected to have correct results");
    } finally {
      executor.shutdownNow();
    }
  }

}
