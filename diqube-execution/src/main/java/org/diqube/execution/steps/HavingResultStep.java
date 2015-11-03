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
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.consumers.AbstractThreadedOverwritingRowIdConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.VersionedExecutionEnvironment;
import org.diqube.queries.QueryRegistry;
import org.diqube.util.Pair;

/**
 * A noop step which provides the results of a HAVING clause on the query master, if available.
 *
 * <p>
 * Input: 1 {@link OverwritingRowIdConsumer} <br>
 * Output: {@link OverwritingRowIdConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class HavingResultStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean sourceIsDone = new AtomicBoolean(false);

  private Pair<ExecutionEnvironment, Long[]> curPair;
  private Object curPairSync = new Object();

  private AbstractThreadedOverwritingRowIdConsumer rowIdConsumer = new AbstractThreadedOverwritingRowIdConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      sourceIsDone.set(true);
    }

    @Override
    protected void doConsume(ExecutionEnvironment env, Long[] rowIds) {
      Pair<ExecutionEnvironment, Long[]> p = new Pair<>(env, rowIds);
      synchronized (curPairSync) {
        if (curPair == null || !(env instanceof VersionedExecutionEnvironment)
            || ((curPair.getLeft() instanceof VersionedExecutionEnvironment) && ((VersionedExecutionEnvironment) env)
                .getVersion() > ((VersionedExecutionEnvironment) curPair.getLeft()).getVersion()))
          curPair = p;
      }
    }
  };

  public HavingResultStep(int stepId, QueryRegistry queryRegistry) {
    super(stepId, queryRegistry);
  }

  @Override
  protected void execute() {
    Pair<ExecutionEnvironment, Long[]> activePair;
    synchronized (curPairSync) {
      activePair = curPair;
    }

    if (activePair != null)
      forEachOutputConsumerOfType(OverwritingRowIdConsumer.class,
          c -> c.consume(activePair.getLeft(), activePair.getRight()));

    if (sourceIsDone.get() && curPair == activePair) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof OverwritingRowIdConsumer))
      throw new IllegalArgumentException("Only OverwritingRowIdConsumer supported!");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(rowIdConsumer);
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

}
