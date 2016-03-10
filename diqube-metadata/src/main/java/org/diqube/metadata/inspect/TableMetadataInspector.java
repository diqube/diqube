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
package org.diqube.metadata.inspect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.metadata.create.FieldNameUtil;
import org.diqube.metadata.inspect.exception.ColumnNameInvalidException;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.TableMetadata;

import com.google.common.collect.Lists;

/**
 * Utility class to help inspect {@link TableMetadata}.
 *
 * @author Bastian Gloeckle
 */
public class TableMetadataInspector {
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;
  private Map<String, FieldMetadata> fieldMetadata;

  /* package */ TableMetadataInspector(TableMetadata metadata,
      RepeatedColumnNameGenerator repeatedColumnNameGenerator) {
    this.repeatedColumnNameGenerator = repeatedColumnNameGenerator;

    fieldMetadata = new HashMap<>();
    for (FieldMetadata f : metadata.getFields())
      fieldMetadata.put(f.getFieldName(), f);
  }

  /**
   * Identifies all interesting {@link FieldMetadata} for a given columnName, as it might be used in a diql query.
   * 
   * <p>
   * The interesting {@link FieldMetadata}s are the ones of the field corresponding to the column itself and all its
   * parent fields.
   * 
   * @param columnName
   *          The column name as it might be used in a diql query. It might contain
   *          {@link RepeatedColumnNameGenerator#allEntriesIdentifyingSubstr()} etc.
   * @return A list of the {@link FieldMetadata} of all fields that are interesting. The list is ordered from the most
   *         generic (parent) to the most specific field.
   * @throws ColumnNameInvalidException
   *           If the columnName is not valid according to the metadata.
   */
  public List<FieldMetadata> findAllFieldMetadata(String columnName) throws ColumnNameInvalidException {
    List<FieldMetadata> m = new ArrayList<>();
    String fieldName = FieldNameUtil.toFieldName(columnName);
    if (columnName.indexOf("..") > -1 || columnName.startsWith(".") || columnName.endsWith("."))
      throw new ColumnNameInvalidException("Column name contains dots at at least one invalid position: " + columnName);

    String firstColName = columnName;
    boolean shouldBeLengthColumn = false;
    if (FieldNameUtil.isLengthColumn(fieldName)) {
      firstColName = columnName.substring(0, columnName.length() - FieldNameUtil.LENGTH.length());
      fieldName = fieldName.substring(0, fieldName.length() - FieldNameUtil.LENGTH.length());
      shouldBeLengthColumn = true;
    }

    findCurrentFieldMetadataAndParents(columnName, firstColName, fieldName, shouldBeLengthColumn, m);
    return Lists.reverse(m);
  }

  /**
   * Processes the column name recursively and finds {@link FieldMetadata} of each field traversed.
   * 
   * @param fullColumnName
   *          The full column name passed to {@link #findAllFieldMetadata(String)}. For error reporting.
   * @param columnName
   *          The current column Name. This method "walks up" the hierarchy of fields.
   * @param fieldName
   *          The current field name as processed by {@link FieldNameUtil#toFieldName(String)} of the column.
   * @param shouldBeLengthColumn
   *          <code>true</code> if the current field is expected to be a [length] column, <code>false</code> if this
   *          would be an error. This is typically true for the full column = the [length] must only appear at the very
   *          end of a full column name
   * @param res
   *          The list to which to add {@link FieldMetadata}. Note that the order is from most specific field to most
   *          generic, as this method "walks up" the field hierarchy.
   * @throws ColumnNameInvalidException
   *           If column name is invalid.
   */
  private void findCurrentFieldMetadataAndParents(String fullColumnName, String columnName, String fieldName,
      boolean shouldBeLengthColumn, List<FieldMetadata> res) throws ColumnNameInvalidException {

    if (!shouldBeLengthColumn && FieldNameUtil.isLengthColumn(columnName))
      throw new ColumnNameInvalidException(
          "Usage of " + FieldNameUtil.LENGTH + " at an illegal position in '" + fullColumnName + "'.");

    if (!fieldMetadata.containsKey(fieldName))
      throw new ColumnNameInvalidException(
          "Field '" + fieldName + "' referenced in column name '" + fullColumnName + "' is not available.");

    FieldMetadata expectedField = fieldMetadata.get(fieldName);

    boolean fieldIsRepeated =
        columnName.endsWith(repeatedColumnNameGenerator.repeatedColumnNameEndsWith()) || shouldBeLengthColumn;
    if (expectedField.isRepeated() && !fieldIsRepeated)
      throw new ColumnNameInvalidException(
          "Field '" + fieldName + "' is repeated, but '" + fullColumnName + "' does not use it that way.");
    else if (!expectedField.isRepeated() && fieldIsRepeated)
      throw new ColumnNameInvalidException(
          "Field '" + fieldName + "' is not repeated, but '" + fullColumnName + "' expects it to be.");

    res.add(expectedField);

    int lastDotColumn = columnName.lastIndexOf('.');
    int lastDotField = fieldName.lastIndexOf('.');
    if (lastDotColumn == -1 ^ lastDotField == -1)
      throw new ColumnNameInvalidException("Internal error while processing '" + fullColumnName + "'.");

    if (lastDotColumn == -1)
      return;

    String nextColumn = columnName.substring(0, lastDotColumn);
    String nextField = fieldName.substring(0, lastDotField);
    findCurrentFieldMetadataAndParents(fullColumnName, nextColumn, nextField, false, res);
  }
}
