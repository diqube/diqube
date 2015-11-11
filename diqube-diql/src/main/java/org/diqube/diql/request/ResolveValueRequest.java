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

import org.diqube.util.ColumnOrValue;

/**
 * Resolve the value of a specific column or literal value.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 * 
 * @author Bastian Gloeckle
 */
public class ResolveValueRequest {
  public ColumnOrValue resolve;

  public ColumnOrValue getResolve() {
    return resolve;
  }

  public void setResolve(ColumnOrValue resolve) {
    this.resolve = resolve;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((resolve == null) ? 0 : resolve.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ResolveValueRequest))
      return false;
    ResolveValueRequest other = (ResolveValueRequest) obj;
    if (resolve == null) {
      if (other.resolve != null)
        return false;
    } else if (!resolve.equals(other.resolve))
      return false;
    return true;
  }

}
