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
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;

import com.google.common.collect.Sets;

/**
 * A logical AND on two row ID steps.
 *
 * TODO #2 STAT if estimated that one source provides lots of rowIds, while the other won't we should first execute the
 * latter and then execute the second with applying the AND right in that step.
 *
 * <p>
 * Input: Exactly two {@link RowIdConsumer}s <br>
 * Output: {@link RowIdConsumer}s
 *
 * @author Bastian Gloeckle
 */
public class RowIdAndStep extends AbstractThreadedExecutablePlanStep {

  private AtomicBoolean leftSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> leftRowIds = new ConcurrentLinkedDeque<>();
  private AtomicBoolean rightSourceIsEmpty = new AtomicBoolean(false);
  private ConcurrentLinkedDeque<Long> rightRowIds = new ConcurrentLinkedDeque<>();
  private Set<Long> rowIdsPolpulatedAlready = new HashSet<>();
  private Set<Long> leftUnmatchedRowIds = new HashSet<>();
  private Set<Long> rightUnmatchedRowIds = new HashSet<>();

  private AbstractThreadedRowIdConsumer leftRowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdAndStep.this.leftSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdAndStep.this.leftRowIds.add(rowId);
    }
  };
  private AbstractThreadedRowIdConsumer rightRowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      RowIdAndStep.this.rightSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        RowIdAndStep.this.rightRowIds.add(rowId);
    }
  };

  public RowIdAndStep(int stepId, QueryRegistry queryRegistry) {
    super(stepId, queryRegistry);
  }

  @Override
  protected void execute() {
    List<Long> newRowIds = new ArrayList<>();
    Long rowId;
    while ((rowId = leftRowIds.poll()) != null)
      leftUnmatchedRowIds.add(rowId);

    while ((rowId = rightRowIds.poll()) != null)
      rightUnmatchedRowIds.add(rowId);

    for (Long rowIdOnBothSides : Sets.intersection(leftUnmatchedRowIds, rightUnmatchedRowIds))
      if (!rowIdsPolpulatedAlready.contains(rowIdOnBothSides))
        newRowIds.add(rowIdOnBothSides);

    if (newRowIds.size() > 0) {
      rowIdsPolpulatedAlready.addAll(newRowIds);
      leftUnmatchedRowIds.removeAll(newRowIds);
      rightUnmatchedRowIds.removeAll(newRowIds);

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
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof RowIdConsumer))
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
      throw new ExecutablePlanBuildException("Could not wire input as only RowIdConsumers are supported.");
  }

}
