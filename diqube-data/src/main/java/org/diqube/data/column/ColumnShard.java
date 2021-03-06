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

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.table.TableShard;

/**
 * A ColumnShard contains all data of one column of all rows of a specific {@link TableShard} or of intermediary values
 * during an execution (see ExecutionEnvironment and VersionedExecutionEnvironment).
 * 
 * <p>
 * From a logical point of view, the ColumnShards of all TableShards with the same name, form together a column.
 * 
 * <p>
 * The sub-interfaces {@link StandardColumnShard} and {@link ConstantColumnShard} provide methods to access the 'Column
 * (Shard) Value IDs' of each row of the column. These Column Value IDs can then be resolved to actual values using the
 * Column Shard Dictionary (see {@link #getColumnShardDictionary()}).
 *
 * <p>
 * A {@link ColumnShard} is typically only valid in the scope of one {@link TableShard} as the name suggests: It
 * contains the values for a specific consecutive range of rowIDs.
 * 
 * <p>
 * For each core data type (String, Long, Double) there are separate sub-interfaces and implementing classes.
 * 
 * @author Bastian Gloeckle
 */
public interface ColumnShard {

  /**
   * @return Name of the column
   */
  public String getName();

  /**
   * @return The dictionary which can resolve 'Column (shard) value IDs' to actual values.
   */
  public Dictionary<?> getColumnShardDictionary();

  /**
   * @return core data type of the column.
   */
  public ColumnType getColumnType();

  /**
   * @return The row ID of the first row this {@link ColumnShard} contains the value of.
   */
  public long getFirstRowId();

  /**
   * @return An approximate number of bytes taken up by this {@link ColumnShard}. Note that this is only an
   *         approximation!
   */
  public long calculateApproximateSizeInBytes();

}
