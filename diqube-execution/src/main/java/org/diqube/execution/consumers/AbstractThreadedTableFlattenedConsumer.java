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

import java.util.Collection;
import java.util.UUID;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.thrift.base.thrift.RNodeAddress;

/**
 * Abstract base class of {@link TableFlattenedConsumer}s that handles calling the {@link ExecutablePlanStep} correctly
 * when there is new data.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractThreadedTableFlattenedConsumer extends AbstractPlanStepBasedGenericConsumer
    implements TableFlattenedConsumer {

  public AbstractThreadedTableFlattenedConsumer(AbstractThreadedExecutablePlanStep planStep) {
    super(planStep);
  }

  @Override
  public void tableFlattened(UUID flattenId, Collection<RNodeAddress> remoteNodes) {
    doTableFlattened(flattenId, remoteNodes);
    if (planStep != null)
      planStep.continueProcessing();
  }

  protected abstract void doTableFlattened(UUID flattenId, Collection<RNodeAddress> remoteNodes);
}
