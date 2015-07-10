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

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.diqube.plan.request.ComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.DelegateComparisonRequest;
import org.diqube.plan.request.ComparisonRequest.Leaf;
import org.diqube.plan.request.ComparisonRequest.Not;
import org.diqube.plan.request.ComparisonRequest.Operator;
import org.diqube.util.Pair;

/**
 * Builds all {@link OptimizerComparisonInfo} on {@link ComparisonRequest}s.
 *
 * @author Bastian Gloeckle
 */
public class OptimizerComparisonInfoEnhancer {
  private ComparisonRequest request;
  private int nextId;

  public OptimizerComparisonInfoEnhancer(ComparisonRequest request) {
    this.request = request;
  }

  public ComparisonRequest enhanceFull() {
    nextId = 0;
    Map<Integer, ComparisonRequest> idToRequest = initializeComparisonInfo();

    updateTransitivelyContainsOnlyEquals(idToRequest);

    return request;
  }

  private void updateTransitivelyContainsOnlyEquals(Map<Integer, ComparisonRequest> idToRequest) {
    Set<Integer> idsAllChildrenEquals = new HashSet<>(idToRequest.keySet());
    Deque<Pair<Integer, Boolean>> allRequests = new LinkedList<>();
    idToRequest.entrySet().stream().filter(e -> e.getValue() instanceof Leaf)
        .forEach(e -> allRequests.add(new Pair<>(e.getKey(), true)));
    while (!allRequests.isEmpty()) {
      Pair<Integer, Boolean> p = allRequests.poll();
      Integer reqId = p.getLeft();
      Boolean allChildrenAreEquals = p.getRight();

      ComparisonRequest req = idToRequest.get(reqId);

      if (req instanceof Leaf)
        allChildrenAreEquals &= ((Leaf) req).getOp().equals(Operator.EQ);

      if (!allChildrenAreEquals)
        idsAllChildrenEquals.remove(reqId);

      if (req.getOptimizerComparisonInfo().getParent() != null)
        allRequests.add(new Pair<>(req.getOptimizerComparisonInfo().getParent().getOptimizerComparisonInfo()
            .getOptimizerId(), allChildrenAreEquals));
    }

    for (Entry<Integer, ComparisonRequest> e : idToRequest.entrySet())
      e.getValue().getOptimizerComparisonInfo()
          .setTransitivelyContainsOnlyEquals(idsAllChildrenEquals.contains(e.getKey()));
  }

  private Map<Integer, ComparisonRequest> initializeComparisonInfo() {
    Map<Integer, ComparisonRequest> idToRequest = new HashMap<>();
    // request/parent pairs
    Deque<Pair<ComparisonRequest, ComparisonRequest>> allRequests = new LinkedList<>();
    allRequests.add(new Pair<>(request, null));
    while (!allRequests.isEmpty()) {
      Pair<ComparisonRequest, ComparisonRequest> p = allRequests.poll();
      ComparisonRequest curReq = p.getLeft();
      ComparisonRequest parent = p.getRight();

      idToRequest.put(nextId, curReq);
      OptimizerComparisonInfo newInfo = new OptimizerComparisonInfo(nextId++);
      newInfo.setParent(parent);
      curReq.setOptimizerComparisonInfo(newInfo);

      if (curReq instanceof DelegateComparisonRequest) {
        allRequests.add(new Pair<>(((DelegateComparisonRequest) curReq).getLeft(), curReq));
        allRequests.add(new Pair<>(((DelegateComparisonRequest) curReq).getRight(), curReq));
      } else if (curReq instanceof Not) {
        allRequests.add(new Pair<>(((Not) curReq).getChild(), curReq));
      }
    }
    return idToRequest;
  }

}
