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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Additional information about a table like the fields it contains and what data types these fields have.
 *
 * <p>
 * This is Serializable in order to be able to send objects of this across the consensus cluster.
 * 
 * <p>
 * Keep in sync with thrift definition in diqube-metadata.
 * 
 * @author Bastian Gloeckle
 */
public class TableMetadata implements Serializable {
  private static final long serialVersionUID = 1L;

  private String tableName;

  private Map<String, FieldMetadata> fields;

  /** for deserialization only */
  public TableMetadata() {
    this.fields = new HashMap<>();
  }

  public TableMetadata(String tableName, Map<String, FieldMetadata> fields) {
    this.tableName = tableName;
    this.fields = fields;
  }

  /**
   * Find the field metadata for a column name.
   * 
   * Note that a field name is not equal to a column name.
   * 
   * @param fullColumnName
   *          column name
   * @return The {@link FieldMetadata} or <code>null</code>.
   */
  public FieldMetadata findFieldMetadata(String fullColumnName) {
    String fieldName = FieldNameUtil.toFieldName(fullColumnName);

    return fields.get(fieldName);
  }

  public String getTableName() {
    return tableName;
  }

  public Map<String, FieldMetadata> getFields() {
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fields == null) ? 0 : fields.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof TableMetadata))
      return false;
    TableMetadata other = (TableMetadata) obj;
    if (fields == null) {
      if (other.fields != null)
        return false;
    } else if (!fields.equals(other.fields))
      return false;
    if (tableName == null) {
      if (other.tableName != null)
        return false;
    } else if (!tableName.equals(other.tableName))
      return false;
    return true;
  }

}
