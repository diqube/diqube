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
package org.diqube.server;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFromRemoteBuilder;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.thrift.RExecutionPlan;

/**
 * TODO implement.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ExecutionPlanExecutor {
  @Inject
  private ExecutablePlanFromRemoteBuilderFactory executablePlanBuilderFactory;

  public RUUID execute(RExecutionPlan executionPlan) {
    RUUID nodeExecutionId = RUuidUtil.toRUuid(UUID.randomUUID());

    ExecutablePlanFromRemoteBuilder executablePlanBuilder =
        executablePlanBuilderFactory.createExecutablePlanFromRemoteBuilder();
    executablePlanBuilder.withRemoteExecutionPlan(executionPlan);
    List<ExecutablePlan> executablePlans = executablePlanBuilder.build();

    return nodeExecutionId;
  }
}
