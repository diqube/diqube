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
package org.diqube.data.column;

import java.util.NavigableMap;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.types.dbl.DoubleColumnShardFactory;
import org.diqube.data.types.dbl.DoubleConstantColumnShard;
import org.diqube.data.types.dbl.DoubleStandardColumnShard;
import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.lng.LongColumnShardFactory;
import org.diqube.data.types.lng.LongConstantColumnShard;
import org.diqube.data.types.lng.LongStandardColumnShard;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.types.str.StringColumnShardFactory;
import org.diqube.data.types.str.StringConstantColumnShard;
import org.diqube.data.types.str.StringStandardColumnShard;
import org.diqube.data.types.str.dict.StringDictionary;

/**
 * A Factory for implementations of {@link ColumnShard}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ColumnShardFactory {

  @Inject
  private StringColumnShardFactory stringColumnShardFactory;

  @Inject
  private LongColumnShardFactory longColumnShardFactory;

  @Inject
  private DoubleColumnShardFactory doubleColumnShardFactory;

  /**
   * Create a new {@link StandardColumnShard} for a string column.
   * 
   * @param colName
   *          Name of the column.
   * @param pages
   *          A map containing the {@link ColumnPage}s of the new column. This object might be empty when calling this
   *          factory method and be filled later on.
   * @param columnShardDictionary
   *          The Column dictionary as described in the JavaDoc of {@link ColumnShard}.
   * @return The new {@link StringStandardColumnShard}.
   */
  public StringStandardColumnShard createStandardStringColumnShard(String colName, NavigableMap<Long, ColumnPage> pages,
      StringDictionary<?> columnShardDictionary) {
    return stringColumnShardFactory.createStandardStringColumnShard(colName, pages, columnShardDictionary);
  }

  /**
   * Create a new {@link ConstantColumnShard} for a string column.
   * 
   * @param colName
   *          Name of the column.
   * @param value
   *          The constant value of the column.
   * @param firstRowId
   *          the first row ID of the column.
   * @return The new {@link StringConstantColumnShard}.
   */
  public StringConstantColumnShard createConstantStringColumnShard(String colName, String value, long firstRowId) {
    return stringColumnShardFactory.createConstantStringColumnShard(colName, value, firstRowId);
  }

  /**
   * Create a new {@link StandardColumnShard} for a long column.
   * 
   * @param colName
   *          Name of the column.
   * @param pages
   *          A map containing the {@link ColumnPage}s of the new column. This object might be empty when calling this
   *          factory method and be filled later on.
   * @param columnShardDictionary
   *          The Column dictionary as described in the JavaDoc of {@link ColumnShard}.
   * @return The new {@link LongStandardColumnShard}.
   */
  public LongStandardColumnShard createStandardLongColumnShard(String colName, NavigableMap<Long, ColumnPage> pages,
      LongDictionary<?> columnShardDictionary) {
    return longColumnShardFactory.createStandardLongColumnShard(colName, pages, columnShardDictionary);
  }

  /**
   * Create a new {@link ConstantColumnShard} for a long column.
   * 
   * @param colName
   *          Name of the column.
   * @param value
   *          The constant value of the column.
   * @param firstRowId
   *          the first row ID of the column.
   * @return The new {@link LongConstantColumnShard}.
   */
  public LongConstantColumnShard createConstantLongColumnShard(String colName, Long value, long firstRowId) {
    return longColumnShardFactory.createConstantLongColumnShard(colName, value, firstRowId);
  }

  /**
   * Create a new {@link StandardColumnShard} for a double column.
   * 
   * @param colName
   *          Name of the column.
   * @param pages
   *          A map containing the {@link ColumnPage}s of the new column. This object might be empty when calling this
   *          factory method and be filled later on.
   * @param columnShardDictionary
   *          The Column dictionary as described in the JavaDoc of {@link ColumnShard}.
   * @return The new {@link DoubleStandardColumnShard}.
   */
  public DoubleStandardColumnShard createStandardDoubleColumnShard(String colName, NavigableMap<Long, ColumnPage> pages,
      DoubleDictionary<?> columnShardDictionary) {
    return doubleColumnShardFactory.createStandardDoubleColumnShard(colName, pages, columnShardDictionary);
  }

  /**
   * Create a new {@link ConstantColumnShard} for a double column.
   * 
   * @param colName
   *          Name of the column.
   * @param value
   *          The constant value of the column.
   * @param firstRowId
   *          the first row ID of the column.
   * @return The new {@link DoubleConstantColumnShard}.
   */
  public DoubleConstantColumnShard createConstantDoubleColumnShard(String colName, Double value, long firstRowId) {
    return doubleColumnShardFactory.createConstantDoubleColumnShard(colName, value, firstRowId);
  }

}
