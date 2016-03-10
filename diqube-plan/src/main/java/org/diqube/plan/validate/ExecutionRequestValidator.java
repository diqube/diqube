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
package org.diqube.plan.validate;

import java.util.Map;

import org.diqube.diql.request.ExecutionRequest;
import org.diqube.plan.PlannerColumnInfo;
import org.diqube.plan.PlannerColumnInfoBuilder;
import org.diqube.plan.exception.ValidationException;

/**
 * Validates an {@link ExecutionRequest}.
 *
 * @author Bastian Gloeckle
 */
public interface ExecutionRequestValidator {
  /**
   * Validates the given {@link ExecutionRequest} of which {@link PlannerColumnInfo}s have been calculated already.
   * 
   * @param executionRequest
   *          The request to validate.
   * @param colInfos
   *          The {@link PlannerColumnInfo}s calculated for all columns in the {@link ExecutionRequest}. Keyed by column
   *          name. Calculated by {@link PlannerColumnInfoBuilder}.
   * @throws ValidationException
   *           Thrown if the request is invalid.
   */
  public void validate(ExecutionRequest executionRequest, Map<String, PlannerColumnInfo> colInfos)
      throws ValidationException;
}
