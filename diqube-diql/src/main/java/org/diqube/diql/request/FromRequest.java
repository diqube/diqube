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

/**
 * The "FROM" clause of a select stmt.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 * 
 * @author Bastian Gloeckle
 */
public class FromRequest {
  private String table;
  private boolean isFlattened;
  private String flattenByField;

  public FromRequest(String table) {
    this.table = table;
    isFlattened = false;
  }

  public FromRequest(String origTable, String flattenByField) {
    table = origTable;
    this.flattenByField = flattenByField;
    isFlattened = true;
  }

  public String getTable() {
    return table;
  }

  public boolean isFlattened() {
    return isFlattened;
  }

  public String getFlattenByField() {
    return flattenByField;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((flattenByField == null) ? 0 : flattenByField.hashCode());
    result = prime * result + (isFlattened ? 1231 : 1237);
    result = prime * result + ((table == null) ? 0 : table.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof FromRequest))
      return false;
    FromRequest other = (FromRequest) obj;
    if (flattenByField == null) {
      if (other.flattenByField != null)
        return false;
    } else if (!flattenByField.equals(other.flattenByField))
      return false;
    if (isFlattened != other.isFlattened)
      return false;
    if (table == null) {
      if (other.table != null)
        return false;
    } else if (!table.equals(other.table))
      return false;
    return true;
  }
}
