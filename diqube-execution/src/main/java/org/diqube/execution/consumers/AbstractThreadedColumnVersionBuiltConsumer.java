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

import java.util.Set;

import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.executionenv.VersionedExecutionEnvironment;

/**
 * Abstract base class of of {@link ColumnVersionBuiltConsumer}.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractThreadedColumnVersionBuiltConsumer extends AbstractPlanStepBasedGenericConsumer
    implements ColumnVersionBuiltConsumer {

  public AbstractThreadedColumnVersionBuiltConsumer(AbstractThreadedExecutablePlanStep planStep) {
    super(planStep);
  }

  @Override
  public void columnVersionBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds) {
    doColumnBuilt(env, colName, adjustedRowIds);
    if (planStep != null)
      planStep.continueProcessing();
  }

  abstract protected void doColumnBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds);

}
