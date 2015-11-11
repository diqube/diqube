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

import java.util.Map;
import java.util.UUID;

import org.diqube.diql.request.ExecutionRequest;

/**
 * Executes general optimizations on an {@link ExecutionRequest}.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionRequestOptimizer {
  private ExecutionRequest request;

  private WhereNotOptimizer whereNotOptimizer = new PushToLeafsWhereNotOptimizer();

  /**
   * @param request
   *          The {@link ExecutionRequest} that should be optimized. This object will be changed when calling
   *          {@link #optimize()}!
   */
  public ExecutionRequestOptimizer(ExecutionRequest request) {
    this.request = request;
  }

  /**
   * Optimizes the {@link ExecutionRequest} provided in the constructor and returns that optimized object.
   */
  public ExecutionRequest optimize() {
    if (request.getWhere() != null) {
      Map<UUID, OptimizerComparisonInfo> info =
          new OptimizerComparisonInfoBuilder().withComparisonRequest(request.getWhere()).build();

      optimizeWhereNot(info);
    }

    return request;
  }

  private ExecutionRequest optimizeWhereNot(Map<UUID, OptimizerComparisonInfo> info) {
    request.setWhere(whereNotOptimizer.optimize(request.getWhere(), info));
    return request;
  }
}
