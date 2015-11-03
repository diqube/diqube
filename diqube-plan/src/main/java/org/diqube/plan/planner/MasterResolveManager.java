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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.executionenv.ExecutionEnvironment;

/**
 * Resolve steps for query master.
 *
 * @author Bastian Gloeckle
 */
public class MasterResolveManager implements ResolveManager<ExecutablePlanStep> {
  private List<ExecutablePlanStep> steps = new ArrayList<ExecutablePlanStep>();
  private Supplier<Integer> nextMasterIdSupplier;
  private ExecutionEnvironment env;
  private ExecutablePlanFactory executablePlanFactory;
  private ColumnManager<ExecutablePlanStep> columnManager;
  private Set<String> resolvedCols = new HashSet<>();
  private MasterWireManager masterWireManager;
  private Set<String> requestedColNames;
  private ExecutablePlanStep columnValueSourceStep;

  public MasterResolveManager(Supplier<Integer> nextMasterIdSupplier, ExecutionEnvironment env,
      ExecutablePlanFactory executablePlanFactory, ColumnManager<ExecutablePlanStep> columnManager,
      MasterWireManager masterWireManager, Set<String> requestedColNames) {
    this.nextMasterIdSupplier = nextMasterIdSupplier;
    this.env = env;
    this.executablePlanFactory = executablePlanFactory;
    this.columnManager = columnManager;
    this.masterWireManager = masterWireManager;
    this.requestedColNames = requestedColNames;
  }

  @Override
  public void resolveValuesOfColumn(String colName) {
    if (resolvedCols.contains(colName))
      return;
    resolvedCols.add(colName);

    ExecutablePlanStep resolveStep =
        executablePlanFactory.createResolveColumnDictIdStep(nextMasterIdSupplier.get(), env, colName);
    columnManager.wireOutputOfColumnIfAvailable(colName, resolveStep);
    steps.add(resolveStep);
  }

  /**
   * @param columnValueSourceStep
   *          The {@link ExecutablePlanStep} that supports a {@link ColumnValueConsumer} output which provides all
   *          values of all resolved columns.
   */
  public void provideColumnValueSourceStep(ExecutablePlanStep columnValueSourceStep) {
    this.columnValueSourceStep = columnValueSourceStep;
  }

  /**
   * Call {@link #provideColumnValueSourceStep(ExecutablePlanStep)} before this!
   */
  @Override
  public List<ExecutablePlanStep> build(ExecutablePlanStep rowIdSourceStep) {
    if (columnValueSourceStep == null)
      throw new IllegalStateException("No column value source specified.");

    // add a filter step that filters out all the columns from the result that were not requested explicitly to be
    // returned by the user.
    ExecutablePlanStep filterValuesStep =
        executablePlanFactory.createFilterRequestedColumnsValuesStep(nextMasterIdSupplier.get(), requestedColNames);
    // consume the column values from the columnValuesSourceStep
    masterWireManager.wire(ColumnValueConsumer.class, columnValueSourceStep, filterValuesStep);
    // make sure to filter all rows that are NOT provided by the rowIdSourceStep - as we might wire the ExecuteRemote
    // step as ColumnValue source, we need to filter out inactive rows, as these might have been merged by a
    // GroupIdAdjustStep.
    masterWireManager.wire(RowIdConsumer.class, rowIdSourceStep, filterValuesStep);

    if (steps.size() > 0) {
      // add a single resolve values step.
      ExecutablePlanStep resolveValueStep = executablePlanFactory.createResolveValuesStep(nextMasterIdSupplier.get());
      // Just to be sure, send the results of that step to the filter step, too.
      masterWireManager.wire(ColumnValueConsumer.class, resolveValueStep, filterValuesStep);

      for (ExecutablePlanStep dictIdStep : steps) {
        // wire input rowID
        masterWireManager.wire(RowIdConsumer.class, rowIdSourceStep, dictIdStep);

        // wire output to resolve value step
        masterWireManager.wire(ColumnDictIdConsumer.class, dictIdStep, resolveValueStep);
      }
      steps.add(resolveValueStep);
    }

    steps.add(filterValuesStep);
    return steps;
  }
}
