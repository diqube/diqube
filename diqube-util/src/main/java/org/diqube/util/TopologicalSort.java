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
package org.diqube.util;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executes a simple topological sort based on a few helper functions.
 *
 * @author Bastian Gloeckle
 */
public class TopologicalSort<T> {

  private Function<T, List<T>> allSuccessorsFunction;
  private Function<T, Long> idFunction;
  private BiConsumer<T, Integer> newIndexConsumer;

  /**
   * 
   * @param allSuccessorsFunction
   *          Find and return all successors to a specific object.
   * @param idFunction
   *          Return a unique ID for a specific object.
   * @param newIndexConsumer
   *          Will be called when the final position for one of the objects was found.
   */
  public TopologicalSort(Function<T, List<T>> allSuccessorsFunction, Function<T, Long> idFunction,
      BiConsumer<T, Integer> newIndexConsumer) {
    this.allSuccessorsFunction = allSuccessorsFunction;
    this.idFunction = idFunction;
    this.newIndexConsumer = newIndexConsumer;
  }

  /**
   * Topologically sort the given values.
   * 
   * @return A topologically sorted list of the objects
   * @throws IllegalArgumentException
   *           If the values cannot be sorted topologically.
   */
  public List<T> sort(List<T> values) throws IllegalArgumentException {
    List<T> res = new ArrayList<>(values.size());

    Map<Long, T> idToValueMap = new HashMap<>();
    Map<Long, List<Long>> successors = new HashMap<>();

    for (T value : values) {
      long id = idFunction.apply(value);
      idToValueMap.put(id, value);
      successors.put(id, allSuccessorsFunction.apply(value).stream().map(idFunction).collect(Collectors.toList()));
    }

    Map<Long, Set<Long>> predecessors = new HashMap<>();
    for (Long id : successors.keySet())
      predecessors.put(id, new HashSet<>());
    for (Entry<Long, List<Long>> successorEntry : successors.entrySet()) {
      for (Long successor : successorEntry.getValue())
        predecessors.get(successor).add(successorEntry.getKey());
    }

    Deque<Long> emptyPredecessors = new LinkedList<Long>(predecessors.entrySet().stream()
        .filter(entry -> entry.getValue().isEmpty()).map(entry -> entry.getKey()).collect(Collectors.toList()));

    while (!emptyPredecessors.isEmpty()) {
      Long id = emptyPredecessors.poll();

      for (Long successorId : successors.get(id)) {
        predecessors.get(successorId).remove(id);
        if (predecessors.get(successorId).isEmpty())
          emptyPredecessors.add(successorId);
      }

      predecessors.remove(id);

      if (newIndexConsumer != null)
        newIndexConsumer.accept(idToValueMap.get(id), res.size());
      res.add(idToValueMap.get(id));
    }

    if (!predecessors.isEmpty())
      throw new IllegalArgumentException("Cyclic dependencies, cannot sort topologically.");

    return res;
  }
}
