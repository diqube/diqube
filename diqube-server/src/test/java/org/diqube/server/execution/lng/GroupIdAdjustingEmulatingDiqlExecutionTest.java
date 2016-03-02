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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.diqube.data.column.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.steps.GroupIdAdjustingStep;
import org.diqube.function.AggregationFunction.ValueProvider;
import org.diqube.function.IntermediaryResult;
import org.diqube.function.aggregate.CountFunction;
import org.diqube.queries.QueryRegistry.QueryResultHandler;
import org.diqube.queries.QueryUuid;
import org.diqube.server.execution.AbstractRemoteEmulatingDiqlExecutionTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests correct data handling of {@link GroupIdAdjustingStep} in case that data is delivered in a "not usual" way.
 *
 * @author Bastian Gloeckle
 */
public class GroupIdAdjustingEmulatingDiqlExecutionTest extends AbstractRemoteEmulatingDiqlExecutionTest<Long> {

  public GroupIdAdjustingEmulatingDiqlExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @Test
  public void groupIdAdjustReceivesUnneededInfoAfterProcessing() throws InterruptedException, ExecutionException {
    // GIVEN
    initializeSampleTableShards(2);

    ExecutablePlan executablePlan = buildExecutablePlan( //
        "Select " + COL_A + ", count() from " + TABLE + //
            " group by " + COL_A + //
            " order by count() desc LIMIT 1");

    ExecutorService executor = executors.newTestExecutor(executablePlan.preferredExecutorServiceSize());
    try {
      // WHEN
      // start execution.
      Future<Void> future = executablePlan.executeAsynchronously(executor);

      QueryResultHandler resultHandler = null;
      while (resultHandler == null) {
        if (queryRegistry.getQueryResultHandlers(QueryUuid.getCurrentQueryUuid()).size() > 0)
          resultHandler = queryRegistry.getQueryResultHandlers(QueryUuid.getCurrentQueryUuid()).iterator().next();
      }

      String countCol = functionBasedColumnNameBuilderFactory.create().withFunctionName("count").build();

      // let first shard returns some values
      Map<Long, Object> values = new HashMap<>();
      values.put(0L, dp.v(5));
      resultHandler.newColumnValues(COL_A, values);
      resultHandler.newIntermediaryAggregationResult(0L, countCol, intermediary(countCol, 0),
          intermediary(countCol, 3));

      // wait until processed
      List<Long> expectedRun1 = Arrays.asList(new Long[] { 0L });
      waitUntilOrFail(newOrderedRowIdsNotify, //
          () -> "Not correct ordering value. Was: " + resultOrderRowIds + " Expected: " + expectedRun1.toString(), //
          () -> resultOrderRowIds.equals(expectedRun1));
      Long expectedValueRun1 = 3L;
      waitUntilOrFail(newValuesNotify, //
          () -> "Not correct value. Was: " + resultValues.get(countCol).get(0L) + " Expected: " + expectedValueRun1, //
          () -> resultValues.get(countCol) != null && expectedValueRun1.equals(resultValues.get(countCol).get(0L)));

      // we send another value for a different col but same row.
      values.clear();
      values.put(0L, dp.v(100));
      resultHandler.newColumnValues(COL_B, values);

      // receive another intermediary of another row. This might have been sent although its value has been cut off by a
      // remote order step for example (so we do not receive actual group-by values but we might have received the group
      // intermediary).
      resultHandler.newIntermediaryAggregationResult(1L, countCol, intermediary(countCol, 0),
          intermediary(countCol, 5));

      // mark as "done", GroupIdAdjusting step should not block (to wait for more data on row 0 or 1).
      resultHandler.oneRemoteDone();
      resultHandler.oneRemoteDone();

      future.get(); // wait until fully done, this should not be killed by the timeout, but should basically return
                    // immediately.

      // final result should only contain the row we had values of the group-by cols and intermediaries.
      Assert.assertEquals(resultOrderRowIds, Arrays.asList(new Long[] { 0L }),
          "Expected final ordering result to be correct");
    } finally {
      executor.shutdownNow();
    }
  }

  private IntermediaryResult intermediary(String outputColName, int count) {
    CountFunction fn = new CountFunction();
    fn.addValues(new ValueProvider<Object>() {
      @Override
      public Object[] getValues() {
        return new Object[count];
      }

      @Override
      public long size() {
        return count;
      }
    });

    IntermediaryResult res = new IntermediaryResult(outputColName, null);
    fn.populateIntermediary(res);
    return res;
  }

}
