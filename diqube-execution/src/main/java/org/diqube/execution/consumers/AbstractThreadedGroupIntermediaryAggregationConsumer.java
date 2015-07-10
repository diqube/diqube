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

import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.function.IntermediaryResult;

/**
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractThreadedGroupIntermediaryAggregationConsumer extends AbstractPlanStepBasedGenericConsumer
    implements GroupIntermediaryAggregationConsumer {

  public AbstractThreadedGroupIntermediaryAggregationConsumer(AbstractThreadedExecutablePlanStep planStep) {
    super(planStep);
  }

  @Override
  public void consumeIntermediaryAggregationResult(long groupId, String colName,
      IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
      IntermediaryResult<Object, Object, Object> newIntermediaryResult) {
    doConsumeIntermediaryAggregationResult(groupId, colName, oldIntermediaryResult, newIntermediaryResult);
    if (planStep != null)
      planStep.continueProcessing();
  }

  abstract protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
      IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
      IntermediaryResult<Object, Object, Object> newIntermediaryResult);

}
