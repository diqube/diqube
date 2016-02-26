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
package org.diqube.data.metadata;

import java.io.Serializable;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnType;

/**
 * Metadata of a field of a table, for example its column type and if it is repeated.
 * 
 * <p>
 * Note that for a repeated field, only one FieldMetadata will be available, although there are multiple
 * {@link ColumnShard}s (i.e. there is a ColumnShard for each index of the repeated field). The field name is stripped
 * of all repetition indices the column name might have (like "[0]", "[5]" or "[length]").
 * 
 * <p>
 * Note that there are also fields for which no direct {@link ColumnShard} is available, as for each column "a.b", "a"
 * is a field, too ({@link FieldType#CONTAINER}), although "a" does not contain any values directly and there is no
 * {@link ColumnShard} therefore.
 *
 * <p>
 * This is Serializable in order to be able to send objects of this across the consensus cluster.
 * 
 * <p>
 * Keep in sync with thrift definition in diqube-metadata
 * 
 * @author Bastian Gloeckle
 */
public class FieldMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Type of a field.
   * 
   * @see ColumnType
   */
  public static enum FieldType {
    STRING, LONG, DOUBLE,
    /** Field itself does not contain data, but is only a container for sub-fields. */
    CONTAINER
  }

  private String fieldName;

  private FieldType fieldType;

  private boolean repeated;

  public FieldMetadata(String fieldName, FieldType fieldType, boolean repeated) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.repeated = repeated;
  }

  public FieldMetadata(FieldMetadata other) {
    this.fieldName = other.fieldName;
    this.fieldType = other.fieldType;
    this.repeated = other.repeated;
  }

  public String getFieldName() {
    return fieldName;
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  public boolean isRepeated() {
    return repeated;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
    result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
    result = prime * result + (repeated ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof FieldMetadata))
      return false;
    FieldMetadata other = (FieldMetadata) obj;
    if (fieldName == null) {
      if (other.fieldName != null)
        return false;
    } else if (!fieldName.equals(other.fieldName))
      return false;
    if (fieldType != other.fieldType)
      return false;
    if (repeated != other.repeated)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "[field: " + fieldName + "; fieldType: " + fieldType + "; repeated: " + repeated + "]";
  }

}
