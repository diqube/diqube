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
package org.diqube.metadata.create;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.TableMetadata;

/**
 * Merges multiple {@link TableMetadata} while validating that they are compatible.
 * 
 * <p>
 * Note that this merger will "union" the data, no input metadata can specify that a specific field of another metadata
 * should be suppressed in the output or something like that!
 * 
 * <p>
 * Merging is associative and commutative.
 *
 * @author Bastian Gloeckle
 */
public class TableMetadataMerger {
  private Collection<TableMetadata> metadata;

  public TableMetadataMerger of(TableMetadata... metadata) {
    this.metadata = Arrays.asList(metadata);
    return this;
  }

  public TableMetadataMerger of(Collection<TableMetadata> metadata) {
    this.metadata = metadata;
    return this;
  }

  public TableMetadata merge() throws IllegalTableLayoutException {
    Map<String, FieldMetadata> finalFields = new HashMap<>();
    String tableName = null;

    for (TableMetadata m : metadata) {
      if (tableName == null)
        tableName = m.getTableName();
      else if (!tableName.equals(m.getTableName()))
        throw new IllegalTableLayoutException("Table names are not equal");

      for (FieldMetadata field : m.getFields()) {
        safePutCopy(field.getFieldName(), field, finalFields);
      }
    }

    return new TableMetadata(tableName, new ArrayList<>(finalFields.values()));
  }

  /**
   * Puts a copy of the given {@link FieldMetadata} into target after validating that the information already in target
   * is compatible with the new one.
   */
  private void safePutCopy(String fieldName, FieldMetadata fieldMetadata, Map<String, FieldMetadata> target)
      throws IllegalTableLayoutException {
    if (target.containsKey(fieldName) && !target.get(fieldName).equals(fieldMetadata))
      throw new IllegalTableLayoutException(
          "Field " + fieldName + " has incompatible metadata: " + fieldMetadata + " <-> " + target.get(fieldName));

    if (!target.containsKey(fieldName))
      target.put(fieldName, new FieldMetadata(fieldMetadata));
  }

  /**
   * The {@link TableMetadata} cannot be merged because the input {@link TableMetadata} contain incompatible
   * information.
   *
   * @author Bastian Gloeckle
   */
  public static class IllegalTableLayoutException extends Exception {
    private static final long serialVersionUID = 1L;

    private IllegalTableLayoutException(String msg) {
      super(msg);
    }

    private IllegalTableLayoutException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
