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
package org.diqube.data.table;

import java.util.Map;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.thrift.v1.STableShard;
import org.diqube.data.types.dbl.DoubleStandardColumnShard;
import org.diqube.data.types.lng.LongStandardColumnShard;
import org.diqube.data.types.str.StringStandardColumnShard;

/**
 * A {@link TableShard} contains all data of a specific consecutive subset of rows of a {@link Table}.
 * 
 * <p>
 * It is made up of various {@link ColumnShard}s each of one of the core data types: String, Long, Double.
 *
 * @author Bastian Gloeckle
 */
public interface TableShard extends DataSerialization<STableShard> {
  /**
   * @return Map from col name to a string col.
   */
  public Map<String, StringStandardColumnShard> getStringColumns();

  /**
   * @return Map from col name to a double col.
   */
  public Map<String, DoubleStandardColumnShard> getDoubleColumns();

  /**
   * @return Map from col name to a long col.
   */
  public Map<String, LongStandardColumnShard> getLongColumns();

  /**
   * @return Map from col name to a col. Contains all columns of the TableShard (= the union of
   *         {@link #getStringColumns()}, {@link #getLongColumns()} and {@link #getDoubleColumns()}).
   */
  public Map<String, StandardColumnShard> getColumns();

  /**
   * @return The number of rows contained in this shard.
   */
  public long getNumberOfRowsInShard();

  /**
   * @return The lowest rowId stored in this TableShard. -1 if {@link #getNumberOfRowsInShard()} == 0.
   */
  public long getLowestRowId();

  /**
   * @return Name of the table this TableShard belongs to.
   */
  public String getTableName();

  /**
   * @return An approximate number of bytes taken up by this {@link ColumnShard}. Note that this is only an
   *         approximation!
   */
  public long calculateApproximateSizeInBytes();
}
