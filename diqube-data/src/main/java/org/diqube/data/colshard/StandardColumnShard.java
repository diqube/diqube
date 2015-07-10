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
package org.diqube.data.colshard;

import java.util.NavigableMap;

import org.diqube.data.TableShard;

/**
 * A {@link StandardColumnShard} contains separate values for each row of a single column of a {@link TableShard}.
 *
 * <p>
 * These values are stored in a set of {@link ColumnPage}s which can be accessed using {@link #getPages()}. Each Page in
 * turn contains a Column Page Dictionary that maps Column Page Value IDs to Column (Shard) Value IDs (see JavaDoc of
 * {@link ColumnShard}), in addition to a value array which contains a Column Page Value ID for each row that is modeled
 * in the {@link ColumnPage}.
 * 
 * <p>
 * Each of the {@link ColumnPage}s contained in a {@link StandardColumnShard} models the values of a specific
 * consecutive range of row IDs.
 * 
 * <p>
 * This type of {@link ColumnShard} is used both, directly in a {@link TableShard} and during execution in temporary
 * columns which are available through an ExecutionEnvironment or VersionedExecutionEnvironment.
 * 
 * <p>
 * Please note that although this type of {@link ColumnShard} maps all rows of a {@link TableShard}, the values of
 * single rows might nevertheless be temporary values (e.g. if fetched during execution from an
 * VersionedExecutionEvironment and if ColumnValueBuiltConsumers are in place), or might be default values created by
 * SparseColumnShardBuilder.
 * 
 * @author Bastian Gloeckle
 */
public interface StandardColumnShard extends ColumnShard {
  /**
   * The {@link ColumnPage}s that effectively contain the Column (Shard) Value IDs which then can be used to resolve
   * actual values using {@link #getColumnShardDictionary()}.
   * <p>
   * Each ColumnPage contains the Column Shard Value IDs of a specific consecutive range of rowIds.
   * 
   * @return Map from a rowId to the {@link ColumnPage} that maps rowIds starting from that first rowID. The set of
   *         returned rowIds will not contain any row ID that is smaller than {@link #getFirstRowId()} and the last
   *         entry of the map will ensure: <code>lastEntry.firstRowId + lastEntry.ColumPage.size() ==
   *         {@link #getNumberOfRowsInColumnShard()} - firstEntry.firstRowId.</code>
   */
  public NavigableMap<Long, ColumnPage> getPages();

  /**
   * @return Number of rows contained in this column shard
   */
  public long getNumberOfRowsInColumnShard();
}
