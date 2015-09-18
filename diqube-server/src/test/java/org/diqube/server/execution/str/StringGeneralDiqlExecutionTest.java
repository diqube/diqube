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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.plan.exception.ParseException;
import org.diqube.server.execution.AbstractCacheDoubleDiqlExecutionTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Validates correct interpretation of String constants in diql.
 *
 * @author Bastian Gloeckle
 */
@Test
public class StringGeneralDiqlExecutionTest extends AbstractCacheDoubleDiqlExecutionTest<String> {

  public StringGeneralDiqlExecutionTest() {
    super(ColumnType.STRING, new StringTestDataProvider());
  }

  @Test
  public void escapeTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = new String[] { "'h'i'" };
    Object[] colBValues = new String[] { "'h'i'" };
    initializeSimpleTable(colAValues, colBValues);
    ExecutablePlan executablePlan = buildExecutablePlan("Select " + COL_A + " from " + TABLE + " where " + //
        COL_B + " = '\\'h\\'i\\''");
    ExecutorService executor = executors.newTestExecutor(executablePlan.preferredExecutorServiceSize());
    try {
      Future<Void> future = executablePlan.executeAsynchronously(executor);
      future.get(); // wait until done.

      Map<String, Map<Long, Object>> expected = new HashMap<>();
      expected.put(COL_A, new HashMap<>());
      expected.get(COL_A).put(0L, "'h'i'");
      Assert.assertEquals(resultValues, expected);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expectedExceptions = ParseException.class)
  public void escapeUnclosedQueryTest() throws InterruptedException, ExecutionException {
    Object[] colAValues = new String[] { "'h'i'" };
    Object[] colBValues = new String[] { "'h'i'" };
    initializeSimpleTable(colAValues, colBValues);
    buildExecutablePlan("Select " + COL_A + " from " + TABLE + " where " + //
        COL_B + " = '\\'h\\'i\\'");
  }

}
