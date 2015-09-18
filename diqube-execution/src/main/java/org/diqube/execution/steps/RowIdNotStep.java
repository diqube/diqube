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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

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
 * A logical NOT on a row ID step.
 * 
 * <p>
 * Executing this step is fairly expensive, as it first needs to collect all row IDs of the input {@link RowIdConsumer}
 * then realize all possible row IDs and them remove those reported. This might take a bit of memory, but definitely
 * slows down the execution as we have to wait first. The optimizer should try to minimize the number of NotSteps.
 * 
 * <p>
 * Input: 1 {@link RowIdConsumer}s. <br>
 * Output: {@link RowIdConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class RowIdNotStep extends AbstractThreadedExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(RowIdNotStep.class);

  private AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> rowIds = new ConcurrentLinkedDeque<>();

  private AbstractThreadedRowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdNotStep.this.sourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdNotStep.this.rowIds.add(rowId);
    }
  };
  private ExecutionEnvironment defaultEnv;

  public RowIdNotStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
  }

  @Override
  public void initialize() {
    if (defaultEnv.getNumberOfRowsInShard() == -1L)
      throw new ExecutablePlanBuildException("NOT step only supported if there's a TableShard.");
  }

  @Override
  protected void execute() {
    if (sourceIsEmpty.get()) {
      long lowestRowId = defaultEnv.getFirstRowIdInShard();
      long numberOfRows = defaultEnv.getNumberOfRowsInShard();

      Set<Long> rowIdSet = new HashSet<Long>(rowIds);

      Long[] resultRowIds = LongStream.range(lowestRowId, lowestRowId + numberOfRows).filter(l -> !rowIdSet.contains(l))
          .mapToObj(Long::valueOf).toArray(l -> new Long[l]);

      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(resultRowIds));
      logger.trace("Reported {} matching rows", resultRowIds.length);
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only RowIdConsumer supported.");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer });
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

}
