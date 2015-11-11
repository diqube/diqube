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
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ComparisonRequest.And;
import org.diqube.diql.request.ComparisonRequest.Leaf;
import org.diqube.diql.request.ComparisonRequest.Not;
import org.diqube.diql.request.ComparisonRequest.Or;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.plan.RemoteExecutionPlanFactory;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;
import org.diqube.util.ColumnOrValue;
import org.diqube.util.Pair;
import org.diqube.util.Triple;

/**
 * Builder that handles {@link ComparisonRequest}s in a WHERE clause - these are fully handled by cluster nodes.
 *
 * @author Bastian Gloeckle
 */
public class WhereBuilder implements ComparisonRequestBuilder<RExecutionPlanStep> {

  private RemoteExecutionPlanFactory remoteExecutionPlanFactory;
  private Supplier<Integer> nextRemoteStepIdSupplier;
  private ColumnManager<RExecutionPlanStep> columnManager;
  private RemoteWireManager remoteWireManager;

  public WhereBuilder(RemoteExecutionPlanFactory remoteExecutionPlanFactory, Supplier<Integer> nextRemoteStepIdSupplier,
      ColumnManager<RExecutionPlanStep> columnManager, RemoteWireManager remoteWireManager) {
    this.remoteExecutionPlanFactory = remoteExecutionPlanFactory;
    this.nextRemoteStepIdSupplier = nextRemoteStepIdSupplier;
    this.columnManager = columnManager;
    this.remoteWireManager = remoteWireManager;
  }

  @Override
  public Pair<RExecutionPlanStep, List<RExecutionPlanStep>> build(ComparisonRequest comparisonRoot) {
    List<RExecutionPlanStep> allWhereSteps = new ArrayList<>();
    RExecutionPlanStep whereRootStep = ComparisonRequestUtil.walkComparisonTreeAndCreate(comparisonRoot,
        new Function<Triple<ComparisonRequest, RExecutionPlanStep, RExecutionPlanStep>, RExecutionPlanStep>() {
          @Override
          public RExecutionPlanStep apply(Triple<ComparisonRequest, RExecutionPlanStep, RExecutionPlanStep> t) {
            RExecutionPlanStep res = null;
            if (t.getLeft() instanceof Leaf) {
              Leaf leaf = (Leaf) t.getLeft();
              RExecutionPlanStepType type = null;
              switch (leaf.getOp()) {
              case EQ:
                type = RExecutionPlanStepType.ROW_ID_EQ;
                break;
              case GT_EQ:
                type = RExecutionPlanStepType.ROW_ID_GT_EQ;
                break;
              case GT:
                type = RExecutionPlanStepType.ROW_ID_GT;
                break;
              case LT_EQ:
                type = RExecutionPlanStepType.ROW_ID_LT_EQ;
                break;
              case LT:
                type = RExecutionPlanStepType.ROW_ID_LT;
                break;
              // TODO #21 support BETWEEN
              }
              res = remoteExecutionPlanFactory.createRowIdComparison(leaf, nextRemoteStepIdSupplier.get(), type);

              // if this step is based on any column that needs to be created (as we're in the where stmt which
              // cannot contain aggregate functions, these columns that need to be created need to be
              // projections!), we need to wait for their creation. Wire accordingly.

              columnManager.wireOutputOfColumnIfAvailable(leaf.getLeftColumnName(), res);

              if (leaf.getRight().getType().equals(ColumnOrValue.Type.COLUMN))
                columnManager.wireOutputOfColumnIfAvailable(leaf.getRight().getColumnName(), res);

              allWhereSteps.add(res);
              return res;
            } else if (t.getLeft() instanceof Not) {
              RExecutionPlanStep childStep = t.getMiddle();

              res = remoteExecutionPlanFactory.createRowIdNot(nextRemoteStepIdSupplier.get());
              remoteWireManager.wire(RowIdConsumer.class, childStep, res);

              allWhereSteps.add(res);
              return res;
            } else if (t.getLeft() instanceof And) {
              res = remoteExecutionPlanFactory.createRowIdAnd(nextRemoteStepIdSupplier.get());
            } else if (t.getLeft() instanceof Or) {
              res = remoteExecutionPlanFactory.createRowIdOr(nextRemoteStepIdSupplier.get());
            }

            // wire row ID flow.
            remoteWireManager.wire(RowIdConsumer.class, t.getMiddle(), res);
            remoteWireManager.wire(RowIdConsumer.class, t.getRight(), res);

            allWhereSteps.add(res);
            return res;
          }
        });
    return new Pair<>(whereRootStep, allWhereSteps);
  }

}
