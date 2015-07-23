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

import java.util.function.Function;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.DelegateComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.Leaf;
import org.diqube.plan.request.ComparisonRequest.Not;
import org.diqube.util.Triple;

/**
 * Util for implementations of {@link ComparisonRequestBuilder}.
 *
 * @author Bastian Gloeckle
 */
public class ComparisonRequestUtil {

  /**
   * Walks along a comparison tree and calls the "processNode" function accordingly.
   * 
   * @param node
   *          The node to start at.
   * @param processNode
   *          Will be called for {@link Leaf} nodes with "node, null, null", for {@link Not} nodes with
   *          "node, childNode, null" and for {@link DelegateComparisonRequest} nodes with
   *          "node, childNodeLeft, childNodeRight". It should return the created {@link ExecutablePlanStep}.
   * @return Root {@link ExecutablePlanStep} which represents the whole tree of {@link ComparisonRequest}s.
   */
  /* package */ static <O> O walkComparisonTreeAndCreate(ComparisonRequest node,
      Function<Triple<ComparisonRequest, O, O>, O> processNode) {
    if (node instanceof Leaf) {
      return processNode.apply(new Triple<>(node, null, null));
    } else if (node instanceof Not) {
      O child = walkComparisonTreeAndCreate(((Not) node).getChild(), processNode);
      return processNode.apply(new Triple<ComparisonRequest, O, O>(node, child, null));
    } else {
      DelegateComparisonRequest del = (DelegateComparisonRequest) node;
      O left = walkComparisonTreeAndCreate(del.getLeft(), processNode);
      O right = walkComparisonTreeAndCreate(del.getRight(), processNode);
      return processNode.apply(new Triple<>(node, left, right));
    }
  }
}
