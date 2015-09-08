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
package org.diqube.execution.steps;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import org.diqube.data.TableShard;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A step that collects the RowIds calculated. If there are no RowId selection steps, this step provides all row IDs of
 * the {@link TableShard} as selection (this happens e.g. if there is no WHERE clause in the stmt). The latter case is
 * only possible on cluster nodes while executing a query, in which case the {@link ExecutionEnvironment} will hold a
 * real {@link TableShard}. This step should therefore <b>not</b> be used on a <b>Query Master</b>.
 * 
 * <p>
 * Input: None or one {@link RowIdConsumer}<br>
 * Output: {@link RowIdConsumer}
 *
 * @author Bastian Gloeckle
 */
public class RowIdSinkStep extends AbstractThreadedExecutablePlanStep {
  private Logger logger = LoggerFactory.getLogger(RowIdSinkStep.class);

  private long numberOfRowsReported = 0L;

  private AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> rowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdSinkStep.this.sourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdSinkStep.this.rowIds.add(rowId);
    }
  };
  private ExecutionEnvironment env;

  public RowIdSinkStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment env) {
    super(stepId, queryRegistry);
    this.env = env;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only RowIdConsumers accepted.");
  }

  @Override
  protected void execute() {
    if (rowIdConsumer.getNumberOfTimesWired() == 0) {
      // If input is not wired, there may be no RowID selection steps in the executable plan (= there is no WHERE
      // clause). We therefore return just all RowIDs.

      // check if we have a TableShard available (will not be the case for QueryMaster, but for cluster nodes executing
      // a query - this step though should not be executed on the query master).
      if (env.getTableShardIfAvailable() != null) {
        long lowestRowId = env.getTableShardIfAvailable().getLowestRowId();
        long numberOfRows = env.getTableShardIfAvailable().getNumberOfRowsInShard();
        if (numberOfRows > 0) {
          // TODO think about not passing an array with all rowIDs.
          Long[] rowIds = LongStream.range(lowestRowId, lowestRowId + numberOfRows).mapToObj(Long::valueOf)
              .toArray(l -> new Long[l]);
          forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(rowIds));

          logger.trace("Reported a total of {} matching rows", rowIds.length);
        }
      }

      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
      return;
    }

    Long[] currentRowIds = new Long[rowIds.size()];
    for (int i = 0; i < currentRowIds.length; i++)
      currentRowIds[i] = rowIds.poll();

    if (currentRowIds.length > 0) {
      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(currentRowIds));
      logger.trace("Reported {} new matching rows", currentRowIds.length);
      numberOfRowsReported += currentRowIds.length;
    }

    if (sourceIsEmpty.get() && rowIds.isEmpty()) {
      logger.trace("Reported a total of {} matching rows", numberOfRowsReported);
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer });
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    // noop as both variants are ok: wired and not wired.
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

}
