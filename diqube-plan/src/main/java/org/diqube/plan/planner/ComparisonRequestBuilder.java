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

import java.util.List;

import org.diqube.plan.request.ComparisonRequest;
import org.diqube.util.Pair;

/**
 * A {@link ComparisonRequestBuilder} builds steps for executing any comparisons of a query. A comparison can either
 * happen in a WHERE clause or in a HAVING clause.
 *
 * @author Bastian Gloeckle
 */
public interface ComparisonRequestBuilder<T> {
  /**
   * Inspect the {@link ComparisonRequest} and build the steps needed. After calling this method, the results are
   * available through {@link #getRootStep()} and {@link #getAllComparisonSteps()}.
   * 
   * @return a Pair containign (1) the resulting root step which can be used to represent the whole comparison and which
   *         internally links to all other steps. And (2) All comparison steps in a flat map.
   */
  public Pair<T, List<T>> build(ComparisonRequest comparisonRoot);
}