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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import org.diqube.execution.consumers.AbstractThreadedOverwritingRowIdConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.queries.QueryRegistry;

/**
 * A logical NOT on a row ID step, based on {@link OverwritingRowIdConsumer}s.
 * 
 * <p>
 * Input: 1 {@link OverwritingRowIdConsumer}s. <br>
 * Output: {@link OverwritingRowIdConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class OverwritingRowIdNotStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean sourceIsDone = new AtomicBoolean(false);
  private Set<Long> rowIds = new ConcurrentSkipListSet<>();
  private ExecutionEnvironment latestEnv = null;
  private Object latestEnvSync = new Object();

  private AbstractThreadedOverwritingRowIdConsumer rowIdConsumer = new AbstractThreadedOverwritingRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      OverwritingRowIdNotStep.this.sourceIsDone.set(true);
    }

    @Override
    protected void doConsume(ExecutionEnvironment env, Long[] rowIds) {
      for (long rowId : rowIds)
        OverwritingRowIdNotStep.this.rowIds.add(rowId);
      synchronized (latestEnvSync) {
        if (latestEnv == null || !(env instanceof VersionedExecutionEnvironment)
            || ((latestEnv instanceof VersionedExecutionEnvironment) && ((VersionedExecutionEnvironment) env)
                .getVersion() > ((VersionedExecutionEnvironment) latestEnv).getVersion()))
          latestEnv = env;
      }
    }
  };

  public OverwritingRowIdNotStep(int stepId, QueryRegistry queryRegistry) {
    super(stepId, queryRegistry);
  }

  @Override
  protected void execute() {
    Set<Long> activeRowIds = new HashSet<>(rowIds);

    ExecutionEnvironment activeEnv;
    synchronized (latestEnvSync) {
      activeEnv = latestEnv;
    }

    if (activeEnv != null) {
      long lastRowId = activeEnv.getLastRowIdInShard();
      Long[] resultRowIds = LongStream.rangeClosed(0L, lastRowId).filter(l -> !activeRowIds.contains(l))
          .mapToObj(Long::valueOf).toArray(l -> new Long[l]);

      forEachOutputConsumerOfType(OverwritingRowIdConsumer.class, c -> c.consume(activeEnv, resultRowIds));
    }

    if (sourceIsDone.get() && rowIds.size() == activeRowIds.size()) {
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
