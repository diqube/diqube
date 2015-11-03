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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.AbstractThreadedOverwritingRowIdConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.VersionedExecutionEnvironment;
import org.diqube.queries.QueryRegistry;

import com.google.common.collect.Sets;

/**
 * A logical AND on two row ID steps and works based on {@link OverwritingRowIdConsumer}s.
 *
 * TODO #2 STAT if estimated that one source provides lots of rowIds, while the other won't we should first execute the
 * latter and then execute the second with applying the AND right in that step.
 * 
 * This is similar to {@link RowIdAndStep}, but works on {@link OverwritingRowIdConsumer}s.
 * 
 * <p>
 * Input: Exactly two {@link OverwritingRowIdConsumer}s <br>
 * Output: {@link OverwritingRowIdConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class OverwritingRowIdAndStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean leftSourceIsDone = new AtomicBoolean(false);
  private Set<Long> leftRowIds = new ConcurrentSkipListSet<>();
  private AtomicBoolean rightSourceIsDone = new AtomicBoolean(false);
  private Set<Long> rightRowIds = new ConcurrentSkipListSet<>();

  private ExecutionEnvironment latestEnv = null;
  private Object latestEnvSync = new Object();

  private AbstractThreadedOverwritingRowIdConsumer leftRowIdConsumer =
      new AbstractThreadedOverwritingRowIdConsumer(this) {
        @Override
        public void allSourcesAreDone() {
          OverwritingRowIdAndStep.this.leftSourceIsDone.set(true);
        }

        @Override
        protected void doConsume(ExecutionEnvironment env, Long[] rowIds) {
          for (long rowId : rowIds)
            OverwritingRowIdAndStep.this.leftRowIds.add(rowId);

          synchronized (latestEnvSync) {
            if (latestEnv == null || !(env instanceof VersionedExecutionEnvironment)
                || ((latestEnv instanceof VersionedExecutionEnvironment) && ((VersionedExecutionEnvironment) env)
                    .getVersion() > ((VersionedExecutionEnvironment) latestEnv).getVersion()))
              latestEnv = env;
          }
        }
      };
  private AbstractThreadedOverwritingRowIdConsumer rightRowIdConsumer =
      new AbstractThreadedOverwritingRowIdConsumer(this) {
        @Override
        public void allSourcesAreDone() {
          OverwritingRowIdAndStep.this.rightSourceIsDone.set(true);
        }

        @Override
        protected void doConsume(ExecutionEnvironment env, Long[] rowIds) {
          for (long rowId : rowIds)
            OverwritingRowIdAndStep.this.rightRowIds.add(rowId);

          synchronized (latestEnvSync) {
            if (latestEnv == null || !(env instanceof VersionedExecutionEnvironment)
                || ((latestEnv instanceof VersionedExecutionEnvironment) && ((VersionedExecutionEnvironment) env)
                    .getVersion() > ((VersionedExecutionEnvironment) latestEnv).getVersion()))
              latestEnv = env;
          }
        }
      };

  public OverwritingRowIdAndStep(int stepId, QueryRegistry queryRegistry) {
    super(stepId, queryRegistry);
  }

  @Override
  protected void execute() {
    int leftRowIdSize = leftRowIds.size();
    int rightRowIdSize = rightRowIds.size();
    Set<Long> res = Sets.intersection(leftRowIds, rightRowIds);
    Long[] resArray = res.toArray(new Long[res.size()]);

    ExecutionEnvironment activeEnv;
    synchronized (latestEnvSync) {
      activeEnv = latestEnv;
    }

    if (activeEnv != null)
      forEachOutputConsumerOfType(OverwritingRowIdConsumer.class, c -> c.consume(activeEnv, resArray));

    if (leftSourceIsDone.get() && rightSourceIsDone.get() && leftRowIds.size() == leftRowIdSize
        && rightRowIds.size() == rightRowIdSize) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof OverwritingRowIdConsumer))
      throw new IllegalArgumentException("Only OverwritingRowIdConsumer supported.");
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { leftRowIdConsumer, rightRowIdConsumer });
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return null;
  }

  @Override
  public void wireOneInputConsumerToOutputOf(Class<? extends GenericConsumer> type, ExecutablePlanStep sourceStep)
      throws ExecutablePlanBuildException {
    if (type.equals(OverwritingRowIdConsumer.class)) {
      if (leftRowIdConsumer.getNumberOfTimesWired() == 0)
        sourceStep.addOutputConsumer(leftRowIdConsumer);
      else if (rightRowIdConsumer.getNumberOfTimesWired() == 0)
        sourceStep.addOutputConsumer(rightRowIdConsumer);
      else
        throw new ExecutablePlanBuildException(
            "Could not wire additional input, because all inputs are wired already.");
    } else
      throw new ExecutablePlanBuildException("Could not wire input as only OverwritingRowIdConsumer are supported.");
  }

}
