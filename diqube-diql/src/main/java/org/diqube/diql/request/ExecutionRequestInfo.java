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
package org.diqube.diql.request;

import java.util.HashSet;
import java.util.Set;

/**
 * Additional information about an {@link ExecutionRequest} that is not strictly needed to execute the request, but is
 * needed to e.g. validate.
 *
 * @author Bastian Gloeckle
 */
public class ExecutionRequestInfo {
  private Set<String> columnNamesRequired = new HashSet<>();

  /**
   * @return Names of the columns that need to be available in the queried table. Does not include "temporary columns"
   *         created by function calls for example. The names of the columns are in exactly the same form as they are
   *         present in the query. This does NOT include the field that is flattened by (if the selection is from a
   *         flattened table), since that field is required in the /original/ table, not the flattened one!
   */
  public Set<String> getColumnNamesRequired() {
    return columnNamesRequired;
  }

}
