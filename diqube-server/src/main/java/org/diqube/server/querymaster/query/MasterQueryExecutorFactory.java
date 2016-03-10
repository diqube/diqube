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
package org.diqube.server.querymaster.query;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.queries.QueryRegistry;
import org.diqube.threads.ExecutorManager;

/**
 * Factory for {@link MasterQueryExecutor}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class MasterQueryExecutorFactory {
  @Inject
  private ExecutorManager executorManager;

  @Inject
  private ExecutionPlanBuilderFactory executionPlanBuildeFactory;

  @Inject
  private QueryRegistry queryRegistry;

  @Inject
  private MasterExecutionRequestValidator masterExecutionRequestValidator;

  public MasterQueryExecutor createExecutor(MasterQueryExecutor.QueryExecutorCallback callback,
      boolean createIntermediaryUpdates) {
    return new MasterQueryExecutor(executorManager, executionPlanBuildeFactory, queryRegistry,
        masterExecutionRequestValidator, callback, createIntermediaryUpdates);
  }
}
