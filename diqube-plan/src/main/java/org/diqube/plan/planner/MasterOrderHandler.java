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
package org.diqube.plan.planner;

import java.util.function.Supplier;

import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.plan.request.OrderRequest;
import org.diqube.util.Pair;

/**
 * Handles {@link OrderRequest} for the query master and creates {@link ExecutablePlanStep}s accordingly.
 *
 * @author Bastian Gloeckle
 */
public class MasterOrderHandler implements OrderRequestBuilder<ExecutablePlanStep> {

  private ExecutablePlanFactory executablePlanFactory;
  private Supplier<Integer> nextMasterStepSupplier;
  private ExecutionEnvironment env;
  private ColumnManager<ExecutablePlanStep> columnManager;

  public MasterOrderHandler(ExecutablePlanFactory executablePlanFactory, Supplier<Integer> nextMasterStepSupplier,
      ExecutionEnvironment env, ColumnManager<ExecutablePlanStep> columnManager) {
    this.executablePlanFactory = executablePlanFactory;
    this.nextMasterStepSupplier = nextMasterStepSupplier;
    this.env = env;
    this.columnManager = columnManager;
  }

  @Override
  public ExecutablePlanStep build(OrderRequest orderRequest) {
    // On Query master we want to order with all requested columns - including any grouped columns.
    ExecutablePlanStep masterOrderStep = executablePlanFactory.createOrderStep(nextMasterStepSupplier.get(), env,
        orderRequest.getColumns(), orderRequest.getLimit(), orderRequest.getLimitStart(), null);

    // If ordering needs to wait for columns to be built, wire that.
    for (Pair<String, Boolean> orderPair : orderRequest.getColumns()) {
      String colName = orderPair.getLeft();
      columnManager.ensureColumnAvailable(colName); // be sure data is available on query master
      columnManager.wireOutputOfColumnIfAvailable(colName, masterOrderStep);
    }

    return masterOrderStep;
  }

}