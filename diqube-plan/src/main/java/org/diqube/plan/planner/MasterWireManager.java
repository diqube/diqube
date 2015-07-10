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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.GenericConsumer;

/**
 * Simple manager for the QueryMaster that handles wiring the {@link ExecutablePlanStep}s.
 * 
 * For doing a topological sort of the steps, we need a corresponding 'link map' (or 'wire map'), in addition to wiring
 * the steps using {@link ExecutablePlanStep#wireOneInputConsumerToOutputOf(ExecutablePlanStep)}. This manager handles
 * that: When planning execution, always use this manager to wire any {@link ExecutablePlanStep}s instead of wiring them
 * directly.
 *
 * @author Bastian Gloeckle
 */
public class MasterWireManager implements WireManager<ExecutablePlanStep> {
  private Map<Integer, Set<Integer>> wires = new HashMap<Integer, Set<Integer>>();

  @Override
  public void wire(Class<? extends GenericConsumer> type, ExecutablePlanStep sourceStep, ExecutablePlanStep destStep) {
    if (!wires.containsKey(sourceStep.getStepId()))
      wires.put(sourceStep.getStepId(), new HashSet<Integer>());
    wires.get(sourceStep.getStepId()).add(destStep.getStepId());
    destStep.wireOneInputConsumerToOutputOf(type, sourceStep);
  }

  public Map<Integer, Set<Integer>> buildFinalWireMap(Collection<ExecutablePlanStep> allMasterSteps) {
    for (ExecutablePlanStep step : allMasterSteps)
      if (!wires.containsKey(step.getStepId()))
        wires.put(step.getStepId(), new HashSet<Integer>());

    return wires;
  }
}