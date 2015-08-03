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
package org.diqube.execution.env.querystats;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.util.Pair;

/**
 * A {@link ColumnShard} facade that provides additional query-methods which will also automatically gather query stats.
 * 
 * Each {@link QueryableColumnShard} is a facade over a "real" {@link ColumnShard} (= either a
 * {@link StandardColumnShard} or a {@link ConstantColumnShard}). The oiginal object can be fetched with
 * {@link QueryableColumnShard#getDelegate()}, but no query stats will be gathered when that object is used.
 * 
 * @author Bastian Gloeckle
 */
public interface QueryableColumnShard extends ColumnShard {
  /**
   * Resolves the column (shard) value IDs of specific rowIds from this column. These column value IDs are IDs in
   * {@link #getColumnShardDictionary()} and can be resolved to actual values using that.
   * 
   * <p>
   * Please note that for rowIds not contained in this column shard, there won't be an entry in the resulting map.
   * 
   * <p>
   * This method will automatically gather query stats.
   * 
   * @return Map from rowID to column value ID.
   */
  public Map<Long, Long> resolveColumnValueIdsForRows(Collection<Long> rowIds);

  /**
   * Just like {@link #resolveColumnValueIdsForRows(Long[])}, but maps the column value ids back into a flat Long[] ->
   * the index of the provided row ID matches the corresponding value in the result array.
   * 
   * <p>
   * Please note that for row IDs that are not available in this column shard, the returned array will be -1.
   * 
   * <p>
   * This method will automatically gather query stats.
   */
  public Long[] resolveColumnValueIdsForRowsFlat(Long[] rowIds);

  /**
   * Just like {@link #resolveColumnValueIdsForRowsFlat(Long[])},but for one rowId only.
   * 
   * <p>
   * This method will automatically gather query stats.
   * 
   * @return -1 if row not available.
   */
  public long resolveColumnValueIdForRow(Long rowId);

  /**
   * Returns information on how rowIds would be well-fitted to be used with the resolve* methods.
   * 
   * @return Set of pair containing firstRowId and length of one bucket.
   */
  public Set<Pair<Long, Integer>> getGoodResolutionPairs();

  /**
   * @return The delegate object to which the facade delegates.
   */
  public ColumnShard getDelegate();
}
