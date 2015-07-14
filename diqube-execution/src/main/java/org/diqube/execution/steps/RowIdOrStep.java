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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;

/**
 * A logical OR on two row ID steps.
 * 
 * <p>
 * Input: Exactly two {@link RowIdConsumer}s. <br>
 * Output: {@link RowIdConsumer}.
 *
 * @author Bastian Gloeckle
 */
public class RowIdOrStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean leftSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> leftRowIds = new ConcurrentLinkedDeque<>();
  private AtomicBoolean rightSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> rightRowIds = new ConcurrentLinkedDeque<>();
  private Set<Long> rowIdsSeenAlready = new HashSet<>();

  private AbstractThreadedRowIdConsumer leftRowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdOrStep.this.leftSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdOrStep.this.leftRowIds.add(rowId);
    }
  };
  private AbstractThreadedRowIdConsumer rightRowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdOrStep.this.rightSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdOrStep.this.rightRowIds.add(rowId);
    }
  };

  public RowIdOrStep(int stepId) {
    super(stepId);
  }

  @Override
  protected void execute() {
    List<Long> newRowIds = new ArrayList<>();
    Long rowId;
    while ((rowId = leftRowIds.poll()) != null)
      if (!rowIdsSeenAlready.contains(rowId))
        newRowIds.add(rowId);

    while ((rowId = rightRowIds.poll()) != null)
      if (!rowIdsSeenAlready.contains(rowId))
        newRowIds.add(rowId);

    if (newRowIds.size() > 0) {
      rowIdsSeenAlready.addAll(newRowIds);
      Long[] rowIdArray = newRowIds.stream().toArray(l -> new Long[l]);
      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(rowIdArray));
    }

    if (leftSourceIsEmpty.get() && rightSourceIsEmpty.get() && leftRowIds.isEmpty() && rightRowIds.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException("Only RowIdConsumer supported.");
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
    if (type.equals(RowIdConsumer.class)) {
      if (leftRowIdConsumer.getNumberOfTimesWired() == 0)
        sourceStep.addOutputConsumer(leftRowIdConsumer);
      else if (rightRowIdConsumer.getNumberOfTimesWired() == 0)
        sourceStep.addOutputConsumer(rightRowIdConsumer);
      else
        throw new ExecutablePlanBuildException(
            "Could not wire additional input, because all inputs are wired already.");
    } else
      throw new ExecutablePlanBuildException(
          "Could not wire input as only RowIdConsumers are supported, but " + type.getSimpleName() + " requested.");
  }

}
