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
package org.diqube.loader;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.diqube.data.ColumnType;

/**
 * Contains information about each column for the loader.
 * 
 * <p>
 * This contains both, a specific {@link ColumnType} for each column and even custom transformation functions. These
 * transformation functions could e.g. be used for a 'date' column: The function parses the date and returns a Long,
 * with the column actually being a Long column internally.
 *
 * @author Bastian Gloeckle
 */
public class LoaderColumnInfo {
  // TODO #14 support optional columns

  public static final Long DEFAULT_LONG = -1L;
  public static final String DEFAULT_STRING = "";
  public static final Double DEFAULT_DOUBLE = 0.;

  private static final Function<String[], Object[]> STRING_COL_FN =
      sa -> Arrays.asList(sa).stream().sequential().map(s -> {
        if (s == null)
          return DEFAULT_STRING;
        return s;
      }).toArray(len -> new String[len]);
  private static final Function<String[], Object[]> LONG_COL_FN = sa -> Arrays.asList(sa).stream().sequential()
      .map(s -> LoaderColumnInfo.parseLong(s)).toArray(len -> new Long[len]);
  private static final Function<String[], Object[]> DOUBLE_COL_FN = sa -> Arrays.asList(sa).stream().sequential()
      .map(s -> LoaderColumnInfo.parseDouble(s)).toArray(len -> new Double[len]);

  private Map<String, ColumnType> columnType = new HashMap<>();
  private Map<String, Function<String[], Object[]>> customTransformationFunction = new HashMap<>();
  private ColumnType defaultColumnType;

  /**
   * Create new {@link LoaderColumnInfo}.
   * 
   * @param defaultColumnType
   *          The column type that will be assumed for columns that have not been registered explicitly.
   */
  public LoaderColumnInfo(ColumnType defaultColumnType) {
    this.defaultColumnType = defaultColumnType;
  }

  /**
   * Register a specific column type for a column without specifying a custom transformation function.
   * 
   * @param colName
   *          Name of the column.
   * @param columnType
   *          The type of the column.
   */
  public void registerColumnType(String colName, ColumnType columnType) {
    this.columnType.put(colName, columnType);
  }

  /**
   * @return The registered {@link ColumnType} for the given column or <code>null</code> if column not yet known.
   */
  public ColumnType getRegisteredColumnType(String colName) {
    return this.columnType.get(colName);
  }

  /**
   * Register a specific column type and a specific transformation function for a column.
   * 
   * @param colName
   *          The name of the column.
   * @param columnType
   *          Internal type of the column.
   * @param transformFunc
   *          A transformation function that will be applied to arrays of input values for the column (all these values
   *          are strings). This function is expected to transform each value of the array and output a resulting array,
   *          preserving the elements order. The resulting arrays items must be matching the {@link ColumnType}
   *          specified (that is, it must be a String[] in case {@link ColumnType#STRING}, a Long[] in case
   *          {@link ColumnType#LONG} and a Double[] in case {@link ColumnType#DOUBLE}). This function must be
   *          thread-safe.
   */
  public void registerCustomTransformationFunc(String colName, ColumnType columnType,
      Function<String[], Object[]> transformFunc) {
    this.columnType.put(colName, columnType);
    this.customTransformationFunction.put(colName, transformFunc);
  }

  /**
   * Return the transformation function for a given column, transforming each input string into a result object
   * according to the columns {@link ColumnType}.
   * 
   * This will return either pre-defined functions or custom defined functions accordingly.
   * 
   * @param column
   *          Name of the column.
   * @return The transformation function. This function is thread-safe.
   */
  public Function<String[], Object[]> getFinalTransformFunc(String column) {
    Function<String[], Object[]> res = customTransformationFunction.get(column);
    if (res != null) {
      return res;
    }
    switch (getFinalColumnType(column)) {
    case STRING:
      return STRING_COL_FN;
    case LONG:
      return LONG_COL_FN;
    case DOUBLE:
      return DOUBLE_COL_FN;
    }
    // never happens
    return null;
  }

  /**
   * Find out if a specific column has a custom data type set or the default is used.
   */
  public boolean isDefaultDataType(String colName) {
    return !columnType.containsKey(colName);
  }

  /**
   * Return {@link ColumnType} to be used for the given column.
   * 
   * Will apply the default column type accordingly.
   * 
   * @param column
   *          Name of the column.
   * @return The {@link ColumnType} that the loader should assume for the values of this column.
   */
  public ColumnType getFinalColumnType(String column) {
    ColumnType res = columnType.get(column);
    if (res == null)
      return defaultColumnType;
    return res;
  }

  public static Long parseLong(String s) {
    if (s == null || "".equals(s))
      // TODO #14 optional columns
      return DEFAULT_LONG;
    return Long.parseLong(s);
  }

  public static Double parseDouble(String s) {
    if (s == null || "".equals(s))
      return DEFAULT_DOUBLE;
    return Double.parseDouble(s);
  }
}
