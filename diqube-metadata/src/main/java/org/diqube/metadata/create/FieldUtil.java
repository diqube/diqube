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

import java.util.regex.Pattern;

import org.diqube.data.column.ColumnType;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;

/**
 *
 * @author Bastian Gloeckle
 */
public class FieldUtil {
  /**
   * Regex pattern that matches all indices between square brackets [], including *. That content needs to be replaced
   * with an empty string to receive the field name. If changed, check also RepeatedColumnNameGenerator.
   * 
   * Does not match "[length]".
   */
  private static final Pattern FULL_COL_NAME_TO_FIELD_NAME_PATTERN = Pattern.compile("\\[[0-9\\*]*\\]");

  /**
   * Keep in sync with RepeatedColumnNameGenerator.
   */
  public static final String LENGTH = "[length]";

  /**
   * Transforms a full column name to the name of the corresponding field name.
   * 
   * Example: All column names "a.b[x]" belong to the same field "a.b".
   */
  public static String toFieldName(String fullColumnName) {
    return FULL_COL_NAME_TO_FIELD_NAME_PATTERN.matcher(fullColumnName).replaceAll("");
  }

  /**
   * @return <code>true</code> for a length column of a repeated field. If the column is a length column, the type of
   *         this length column does not necessarily match the type of the actual column (length cols are always LONG).
   */
  public static boolean columnTypeMightDifferFromFieldType(String fullColumnName) {
    return isLengthColumn(fullColumnName);
  }

  /**
   * @return <code>true</code> if the column is a "length" column of a repeated field.
   */
  public static boolean isLengthColumn(String fullColumnName) {
    return fullColumnName.endsWith(LENGTH);
  }

  public static FieldType toFieldType(ColumnType colType) {
    switch (colType) {
    case STRING:
      return FieldType.STRING;
    case LONG:
      return FieldType.LONG;
    case DOUBLE:
      return FieldType.DOUBLE;
    }
    return null;
  }

  public static ColumnType toColumnType(FieldMetadata m) {
    switch (m.getFieldType()) {
    case STRING:
      return ColumnType.STRING;
    case LONG:
      return ColumnType.LONG;
    case DOUBLE:
      return ColumnType.DOUBLE;
    default:
      return null;
    }
  }
}
