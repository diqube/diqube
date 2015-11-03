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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.OverwritingRowIdConsumer;
import org.diqube.execution.steps.RowIdInequalStep;
import org.diqube.execution.steps.RowIdInequalStep.RowIdComparator;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.And;
import org.diqube.plan.request.ComparisonRequest.Leaf;
import org.diqube.plan.request.ComparisonRequest.Not;
import org.diqube.plan.request.ComparisonRequest.Operator;
import org.diqube.plan.request.ComparisonRequest.Or;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;
import org.diqube.util.Triple;

/**
 * Builder that handles {@link ComparisonRequest}s in a HAVING clause - these are fully handled by the query master.
 *
 * @author Bastian Gloeckle
 */
public class HavingBuilder implements ComparisonRequestBuilder<ExecutablePlanStep> {

  private ExecutablePlanFactory executablePlanFactory;
  private Supplier<Integer> nextMasterStepIdSupplier;
  private ColumnManager<ExecutablePlanStep> columnManager;
  private ExecutionEnvironment env;
  private MasterWireManager masterWireManager;

  public HavingBuilder(ExecutablePlanFactory executablePlanFactory, Supplier<Integer> nextMasterStepIdSupplier,
      ColumnManager<ExecutablePlanStep> columnManager, ExecutionEnvironment env, MasterWireManager masterWireManager) {
    this.executablePlanFactory = executablePlanFactory;
    this.nextMasterStepIdSupplier = nextMasterStepIdSupplier;
    this.columnManager = columnManager;
    this.env = env;
    this.masterWireManager = masterWireManager;
  }

  @Override
  public Pair<ExecutablePlanStep, List<ExecutablePlanStep>> build(ComparisonRequest comparisonRoot) {
    List<ExecutablePlanStep> allHavingSteps = new ArrayList<>();
    ExecutablePlanStep havingRootStep = ComparisonRequestUtil.walkComparisonTreeAndCreate(comparisonRoot,
        new Function<Triple<ComparisonRequest, ExecutablePlanStep, ExecutablePlanStep>, ExecutablePlanStep>() {
          @Override
          public ExecutablePlanStep apply(Triple<ComparisonRequest, ExecutablePlanStep, ExecutablePlanStep> t) {
            ExecutablePlanStep res = null;
            if (t.getLeft() instanceof Leaf) {
              Leaf leaf = (Leaf) t.getLeft();
              if (leaf.getOp().equals(Operator.EQ)) {
                String colName = leaf.getLeftColumnName();
                if (leaf.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
                  res = executablePlanFactory.createRowIdEqualsStep(nextMasterStepIdSupplier.get(), env, colName,
                      leaf.getRight().getColumnName());
                else {
                  Object[] values = (Object[]) Array.newInstance(leaf.getRight().getValue().getClass(), 1);
                  values[0] = leaf.getRight().getValue();
                  res =
                      executablePlanFactory.createRowIdEqualsStep(nextMasterStepIdSupplier.get(), env, colName, values);
                }
              } else {
                RowIdComparator comparator = null;
                switch (leaf.getOp()) {
                case GT_EQ:
                  comparator = new RowIdInequalStep.GtEqRowIdComparator();
                  break;
                case GT:
                  comparator = new RowIdInequalStep.GtRowIdComparator();
                  break;
                case LT_EQ:
                  comparator = new RowIdInequalStep.LtEqRowIdComparator();
                  break;
                case LT:
                  comparator = new RowIdInequalStep.LtRowIdComparator();
                  break;
                default:
                }
                String colName = leaf.getLeftColumnName();

                if (leaf.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
                  res = executablePlanFactory.createRowIdInequalStep2Cols(nextMasterStepIdSupplier.get(), env, colName,
                      leaf.getRight().getColumnName(), comparator);
                else
                  res = executablePlanFactory.createRowIdInequalStep(nextMasterStepIdSupplier.get(), env, colName,
                      leaf.getRight().getValue(), comparator);
              }

              // we're on the query master when executing this and the having clause is definitely based on aggregated
              // columns (this is validated that way!). This means that the aggregation columns and the projected
              // columns based on this should already be available on the query master. So we only need to wire
              // correctly, we do not need to "ensure columns are available".
              columnManager.wireOutputOfColumnIfAvailable(leaf.getLeftColumnName(), res);

              if (leaf.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
                columnManager.wireOutputOfColumnIfAvailable(leaf.getRight().getColumnName(), res);

              allHavingSteps.add(res);
              return res;
            } else if (t.getLeft() instanceof Not) {
              ExecutablePlanStep childStep = t.getMiddle();

              res = executablePlanFactory.createOverwritingRowIdNotStep(nextMasterStepIdSupplier.get());
              masterWireManager.wire(OverwritingRowIdConsumer.class, childStep, res);

              allHavingSteps.add(res);
              return res;
            } else if (t.getLeft() instanceof And) {
              res = executablePlanFactory.createOverwritingRowIdAndStep(nextMasterStepIdSupplier.get());
            } else if (t.getLeft() instanceof Or) {
              res = executablePlanFactory.createOverwritingRowIdOrStep(nextMasterStepIdSupplier.get());
            }

            // wire row ID flow.
            masterWireManager.wire(OverwritingRowIdConsumer.class, t.getMiddle(), res);
            masterWireManager.wire(OverwritingRowIdConsumer.class, t.getRight(), res);

            allHavingSteps.add(res);
            return res;
          }
        });
    return new Pair<>(havingRootStep, allHavingSteps);
  }

}
