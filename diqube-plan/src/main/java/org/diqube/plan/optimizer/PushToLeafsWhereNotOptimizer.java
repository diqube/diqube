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
package org.diqube.plan.optimizer;

import java.util.HashMap;
import java.util.Map;

import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.And;
import org.diqube.plan.request.ComparisonRequest.DelegateComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.Leaf;
import org.diqube.plan.request.ComparisonRequest.Not;
import org.diqube.plan.request.ComparisonRequest.Operator;
import org.diqube.plan.request.ComparisonRequest.Or;
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
  public ComparisonRequest optimize(ComparisonRequest request) {
    logger.debug("Optimizing Comparison Request: {}", request);
    ComparisonRequest res = optimizeRecursive(request);
    logger.debug("Optimized Comparison Request: {}", res);
    return res;
  }

  private ComparisonRequest optimizeRecursive(ComparisonRequest request) {
    if (request.getOptimizerComparisonInfo().isTransitivelyContainsOnlyEquals())
      // do not "optimize" sub-trees which only contain "=" comparisons, as we cannot optimize those anyway.
      // TODO #2 STAT base this optimization on statistics about the amount of data to be expected
      return request;

    if (request instanceof Not) {
      Not not = (Not) request;
      if (not.getChild() instanceof Not)
        // double not -> remove.
        return optimizeRecursive(((Not) not.getChild()).getChild());

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
        new OptimizerComparisonInfoEnhancer(notLeft).enhanceFull();
        new OptimizerComparisonInfoEnhancer(notRight).enhanceFull();

        del.setLeft(optimizeRecursive(notLeft));
        del.setRight(optimizeRecursive(notRight));

        new OptimizerComparisonInfoEnhancer(del).enhanceFull();

        return del;
      }
    } else if (request instanceof DelegateComparisonRequest) {
      DelegateComparisonRequest del = (DelegateComparisonRequest) request;
      del.setLeft(optimizeRecursive(del.getLeft()));
      del.setRight(optimizeRecursive(del.getRight()));
      new OptimizerComparisonInfoEnhancer(del).enhanceFull();
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
