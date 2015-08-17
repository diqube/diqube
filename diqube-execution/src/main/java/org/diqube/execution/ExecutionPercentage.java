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
package org.diqube.execution;

import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.execution.consumers.AbstractDoneConsumer;

/**
 * Attaches to a {@link ExecutablePlan} and gathers information on how much percent of the plan have been executed
 * already.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionPercentage {
  private int numberOfStepsTotal;
  private ExecutablePlan executablePlan;
  private AtomicInteger numberOfStepsDone = new AtomicInteger(0);
  private boolean isAttached = false;

  /**
   * Create a {@link ExecutablePlan} for the given plan. Be sure to call {@link #attach()}!.
   */
  public ExecutionPercentage(ExecutablePlan executablePlan) {
    this.executablePlan = executablePlan;
    numberOfStepsTotal = executablePlan.getSteps().size();
  }

  /**
   * Attaches to the executable plan in order to be able to record the percentages. This should be called before the
   * executable plan is started to be executed!
   */
  public void attach() {
    for (ExecutablePlanStep step : executablePlan.getSteps()) {
      step.addOutputConsumer(new AbstractDoneConsumer() {
        @Override
        public void sourceIsDone() {
          numberOfStepsDone.incrementAndGet();
        }
      });
    }
    isAttached = true;
  }

  /**
   * Calculate the current percentage on how much of the executable plan is done. Call {@link #attach()} before!
   * 
   * @throws IllegalStateException
   *           if not attached.
   */
  public short calculatePercentDone() throws IllegalStateException {
    if (!isAttached)
      throw new IllegalStateException("Not attached.");

    if (numberOfStepsTotal == 0)
      return (short) 100;
    return (short) (numberOfStepsDone.get() * 100 / numberOfStepsTotal);
  }
}
