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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.server.execution.GroupDiqlExecutionTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
@Test
public class StringGroupDiqlExecutionTest extends GroupDiqlExecutionTest<String> {

  public StringGroupDiqlExecutionTest() {
    super(ColumnType.STRING, new StringTestDataProvider());
  }

  @Test
  public void aggregationFunctionWithConstantParam() throws InterruptedException, ExecutionException {
    Object[] colAValues = dp.a(1, 5, 100, 1, 99, 1);
    Object[] colBValues = dp.a(3, 0, 0, 2, 0, 10);
    initializeSimpleTable(colAValues, colBValues);
    // GIVEN
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " group by " + COL_A
        + " having any('" + dp.v(10) + "', " + COL_B + ") = 1");
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

      Assert.assertEquals(resultValues.get(COL_A).get(resultHavingRowIds[0]), dp.v(1), "Expected correct value.");
    } finally {
      executor.shutdownNow();
    }
  }
}
