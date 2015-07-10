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

import java.util.Map;

import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;

/**
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractThreadedColumnValueConsumer extends AbstractPlanStepBasedGenericConsumer implements
    ColumnValueConsumer {

  public AbstractThreadedColumnValueConsumer(AbstractThreadedExecutablePlanStep planStep) {
    super(planStep);
  }

  @Override
  public void consume(String colName, Map<Long, Object> values) {
    doConsume(colName, values);
    if (planStep != null)
      planStep.continueProcessing();
  }

  protected abstract void doConsume(String colName, Map<Long, Object> values);
}
