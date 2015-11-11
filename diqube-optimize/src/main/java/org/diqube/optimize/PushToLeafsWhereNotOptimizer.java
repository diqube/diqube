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
package org.diqube.optimize;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ComparisonRequest.And;
import org.diqube.diql.request.ComparisonRequest.DelegateComparisonRequest;
import org.diqube.diql.request.ComparisonRequest.Leaf;
import org.diqube.diql.request.ComparisonRequest.Not;
import org.diqube.diql.request.ComparisonRequest.Operator;
import org.diqube.diql.request.ComparisonRequest.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes the 'not's in a WHERE clause in that way, that it pushes the NOTs as far down the comparison tree as
 * possible.
 * 
 * <p>
 * RowIdNotSteps block until their RowIdConsumer input is fully done - this means that they hinder simultaneous
 * execution. Using this approach, we might end up having more Not steps than before, but we should be able to
 * parallelize better. In addition to that, we end up having Not steps only on RowIdEqual steps, because we can just
 * switch the operator on RowIdInequal steps to build a NOT. Additionally, each Not step might take quite some amount of
 * memory, as it materializes all rowIDs except those that were reported by subsequent steps - which might be a few...
 *
 * @author Bastian Gloeckle
 */
public class PushToLeafsWhereNotOptimizer implements WhereNotOptimizer {

  private static final Logger logger = LoggerFactory.getLogger(PushToLeafsWhereNotOptimizer.class);

  /**
   * When arriving at inequality comparisons with a NOT operator in front, we can optimize that by removing the not and
   * changing the comparison operator. Example:
   * 
   * <code>NOT a > b</code> == <code>a <= b</code>
   * 
   * This map holds the mapping of the operators when removing the NOT.
   */
  private static final Map<Operator, Operator> notOperator;

  @Override
  public ComparisonRequest optimize(ComparisonRequest request, Map<UUID, OptimizerComparisonInfo> info) {
    logger.debug("Optimizing Comparison Request: {}", request);
    ComparisonRequest res = optimizeRecursive(request, info);
    logger.debug("Optimized Comparison Request: {}", res);
    return res;
  }

  /**
   * @param info
   *          Gets adjusted while optimizing. (Note: be sure that each recursive call uses the same object!).
   */
  private ComparisonRequest optimizeRecursive(ComparisonRequest request, Map<UUID, OptimizerComparisonInfo> info) {
    if (info.get(request.getVirtualId()).isTransitivelyContainsOnlyEquals())
      // do not "optimize" sub-trees which only contain "=" comparisons, as we cannot optimize those anyway.
      // TODO #2 STAT base this optimization on statistics about the amount of data to be expected
      return request;

    if (request instanceof Not) {
      Not not = (Not) request;
      if (not.getChild() instanceof Not)
        // double not -> remove.
        return optimizeRecursive(((Not) not.getChild()).getChild(), info);

      if (not.getChild() instanceof Leaf) {
        Leaf leaf = (Leaf) not.getChild();
        if (leaf.getOp().equals(Operator.EQ))
          // we cannot optimize "NOT a = b".
          return not;

        // It's a <, <=, >, >= comparison. Optimize by removing the NOT and switching the operator of the comparison.
        // TODO #29 this might actually lead to having more rowIds and more work to be done by the inequality steps.
        leaf.setOp(notOperator.get(leaf.getOp()));
        return leaf;
      }

      if (not.getChild() instanceof DelegateComparisonRequest) {
        // move NOT further down the tree.
        DelegateComparisonRequest del = (DelegateComparisonRequest) not.getChild();

        Not notLeft = new Not();
        notLeft.setChild(del.getLeft());
        Not notRight = new Not();
        notRight.setChild(del.getRight());

        if (del instanceof And)
          del = new Or();
        else
          del = new And();

        // TODO do no full enhancement here.
        info.putAll(new OptimizerComparisonInfoBuilder().withComparisonRequest(notLeft).build());
        info.putAll(new OptimizerComparisonInfoBuilder().withComparisonRequest(notRight).build());

        del.setLeft(optimizeRecursive(notLeft, info));
        del.setRight(optimizeRecursive(notRight, info));

        info.putAll(new OptimizerComparisonInfoBuilder().withComparisonRequest(del).build());

        return del;
      }
    } else if (request instanceof DelegateComparisonRequest) {
      DelegateComparisonRequest del = (DelegateComparisonRequest) request;
      del.setLeft(optimizeRecursive(del.getLeft(), info));
      del.setRight(optimizeRecursive(del.getRight(), info));
      info.putAll(new OptimizerComparisonInfoBuilder().withComparisonRequest(del).build());
      return del;
    }
    return request;
  }

  static {
    notOperator = new HashMap<>();
    notOperator.put(Operator.GT, Operator.LT_EQ);
    notOperator.put(Operator.GT_EQ, Operator.LT);
    notOperator.put(Operator.LT, Operator.GT_EQ);
    notOperator.put(Operator.LT_EQ, Operator.GT);
  }
}
