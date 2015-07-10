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

import org.diqube.plan.request.ComparisonRequest;

/**
 * Optimizes the uses of "not" in a WHERE clause.
 *
 * @author Bastian Gloeckle
 */
public interface WhereNotOptimizer {
  /**
   * Optimize the given {@link ComparisonRequest} and return the optimized object (might be a different one than the one
   * provided).
   * 
   * @param request
   *          A {@link ComparisonRequest} with {@link OptimizerComparisonInfo}s available (see
   *          {@link OptimizerComparisonInfoEnhancer}).
   */
  public ComparisonRequest optimize(ComparisonRequest request);
}
