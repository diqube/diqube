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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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

}
