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

/**
 * Either a reference to a column name or a literal value.
 *
 * <p>
 * Correctly implements {@link Object#equals(Object)} and {@link Object#hashCode()}.
 * 
 * @author Bastian Gloeckle
 */
public class ColumnOrValue {
  public static enum Type {
    COLUMN, LITERAL
  }

  private ColumnOrValue.Type type;
  private String columnName;
  private Object value;

  public ColumnOrValue(ColumnOrValue.Type type, Object value) {
    this.type = type;
    if (type.equals(Type.COLUMN))
      columnName = (String) value;
    else {
      this.value = value;
    }
  }

  public ColumnOrValue.Type getType() {
    return type;
  }

  public String getColumnName() {
    return columnName;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    if (type.equals(Type.COLUMN))
      return "Col[" + columnName + "]";
    return "Val[" + value + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ColumnOrValue))
      return false;
    ColumnOrValue other = (ColumnOrValue) obj;
    if (columnName == null) {
      if (other.columnName != null)
        return false;
    } else if (!columnName.equals(other.columnName))
      return false;
    if (type != other.type)
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }
}