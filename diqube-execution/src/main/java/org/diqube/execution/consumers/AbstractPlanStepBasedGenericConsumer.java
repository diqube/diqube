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
package org.diqube.execution.consumers;

import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractPlanStepBasedGenericConsumer implements GenericConsumer {

  private static final Logger logger = LoggerFactory.getLogger(AbstractPlanStepBasedGenericConsumer.class);

  /** The step this consumer belongs to, could be <code>null</code> for unit tests. */
  protected AbstractThreadedExecutablePlanStep planStep;

  protected AtomicInteger wireDelta = new AtomicInteger(0);
  protected AtomicInteger wireCount = new AtomicInteger(0);

  public AbstractPlanStepBasedGenericConsumer(AbstractThreadedExecutablePlanStep planStep) {
    this.planStep = planStep;
  }

  @Override
  public Integer getDestinationPlanStepId() {
    if (planStep == null)
      return null;
    return planStep.getStepId();
  }

  @Override
  public void sourceIsDone() {
    if (wireDelta.decrementAndGet() == 0) {
      logger.trace("All sources done on {}/{}", planStep != null ? planStep.getClass().getSimpleName() : "noname",
          this.getType());
      allSourcesAreDone();
    }
    if (planStep != null)
      planStep.continueProcessing();
  }

  abstract protected void allSourcesAreDone();

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[destinationPlanStepId=" + getDestinationPlanStepId() + "]";
  }

  @Override
  public void recordOneWiring() {
    wireDelta.incrementAndGet();
    wireCount.incrementAndGet();
  }

  @Override
  public int getNumberOfTimesWired() {
    return wireCount.get();
  }

  @Override
  public int getNumberOfActiveWirings() {
    return wireDelta.get();
  }

}
