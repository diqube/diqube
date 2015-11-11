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

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.diqube.diql.request.ComparisonRequest;
import org.diqube.diql.request.ComparisonRequest.DelegateComparisonRequest;
import org.diqube.diql.request.ComparisonRequest.Leaf;
import org.diqube.diql.request.ComparisonRequest.Not;
import org.diqube.diql.request.ComparisonRequest.Operator;
import org.diqube.util.Pair;

/**
 * Builds all {@link OptimizerComparisonInfo} on {@link ComparisonRequest}s.
 *
 * @author Bastian Gloeckle
 */
public class OptimizerComparisonInfoBuilder {
  private ComparisonRequest request;

  public OptimizerComparisonInfoBuilder() {
  }

  public OptimizerComparisonInfoBuilder withComparisonRequest(ComparisonRequest request) {
    this.request = request;
    return this;
  }

  /**
   * @return map from {@link ComparisonRequest#getVirtualId()} to {@link OptimizerComparisonInfo} belonging to it.
   */
  public Map<UUID, OptimizerComparisonInfo> build() {
    Pair<Map<UUID, ComparisonRequest>, Map<UUID, OptimizerComparisonInfo>> p = initialize();

    updateTransitivelyContainsOnlyEquals(p.getLeft(), p.getRight());

    return p.getRight();
  }

  private void updateTransitivelyContainsOnlyEquals(Map<UUID, ComparisonRequest> idToRequest,
      Map<UUID, OptimizerComparisonInfo> idToOptimizerInfo) {
    Set<UUID> idsAllChildrenEquals = new HashSet<>(idToRequest.keySet());

    // Pair: VirtualId of request to "all children are equals".
    Deque<Pair<UUID, Boolean>> allRequests = new LinkedList<>();
    idToRequest.entrySet().stream().filter(e -> e.getValue() instanceof Leaf)
        .forEach(e -> allRequests.add(new Pair<>(e.getKey(), true)));
    while (!allRequests.isEmpty()) {
      Pair<UUID, Boolean> p = allRequests.poll();
      UUID reqId = p.getLeft();
      Boolean allChildrenAreEquals = p.getRight();

      ComparisonRequest req = idToRequest.get(reqId);

      if (req instanceof Leaf)
        allChildrenAreEquals &= ((Leaf) req).getOp().equals(Operator.EQ);

      if (!allChildrenAreEquals)
        idsAllChildrenEquals.remove(reqId);

      OptimizerComparisonInfo info = idToOptimizerInfo.get(req.getVirtualId());

      if (info.getParent() != null)
        allRequests.add(new Pair<>(info.getParent().getVirtualId(), allChildrenAreEquals));
    }

    for (Entry<UUID, ComparisonRequest> e : idToRequest.entrySet())
      idToOptimizerInfo.get(e.getKey()).setTransitivelyContainsOnlyEquals(idsAllChildrenEquals.contains(e.getKey()));
  }

  private Pair<Map<UUID, ComparisonRequest>, Map<UUID, OptimizerComparisonInfo>> initialize() {
    Map<UUID, ComparisonRequest> idToRequest = new HashMap<>();
    Map<UUID, OptimizerComparisonInfo> idToOptimizerInfo = new HashMap<>();
    // request/parent pairs
    Deque<Pair<ComparisonRequest, ComparisonRequest>> allRequests = new LinkedList<>();
    allRequests.add(new Pair<>(request, null));
    while (!allRequests.isEmpty()) {
      Pair<ComparisonRequest, ComparisonRequest> p = allRequests.poll();
      ComparisonRequest curReq = p.getLeft();
      ComparisonRequest parent = p.getRight();

      idToRequest.put(curReq.getVirtualId(), curReq);
      OptimizerComparisonInfo newInfo = new OptimizerComparisonInfo(curReq.getVirtualId());
      newInfo.setParent(parent);
      idToOptimizerInfo.put(curReq.getVirtualId(), newInfo);

      if (curReq instanceof DelegateComparisonRequest) {
        allRequests.add(new Pair<>(((DelegateComparisonRequest) curReq).getLeft(), curReq));
        allRequests.add(new Pair<>(((DelegateComparisonRequest) curReq).getRight(), curReq));
      } else if (curReq instanceof Not) {
        allRequests.add(new Pair<>(((Not) curReq).getChild(), curReq));
      }
    }
    return new Pair<>(idToRequest, idToOptimizerInfo);
  }

}
