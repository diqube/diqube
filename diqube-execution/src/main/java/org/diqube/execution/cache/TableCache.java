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
package org.diqube.execution.cache;

import java.util.Collection;

import org.diqube.data.Table;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;

/**
 * Caches objects in the context of a {@link Table}.
 * 
 * <p>
 * Typically, objects that need to be cached belong to one {@link TableShard} of the table. To get better
 * maintainability of the cache we though chose to cache based on a table (and evict no-longer needed cached objects
 * based on a whole Table, not a {@link TableShard}). To manage cached objects for a specific {@link TableShard}, this
 * class typically uses the {@link TableShard#getLowestRowId()} to identify a specific TableShard to get the cached
 * value of.
 * 
 * @author Bastian Gloeckle
 */
public interface TableCache {
  /**
   * Returns a specific cached {@link ColumnShard} of a TableShard inside this table if available.
   * 
   * @param firstRowIdTableShard
   *          The {@link TableShard#getLowestRowId()} of the TableShard of which to return a cached {@link ColumnShard}
   *          with the given name.
   * @param colName
   *          The name of the column that is cached.
   * @return The cached {@link ColumnShard} or <code>null</code> if not available.
   */
  public ColumnShard getCachedColumnShard(long firstRowIdTableShard, String colName);

  /**
   * Returns all cached {@link ColumnShard}s of a TableShard inside this table if available.
   * 
   * @param firstRowIdTableShard
   *          The {@link TableShard#getLowestRowId()} of the TableShard of which to return a cached {@link ColumnShard}s
   *          with the given name.
   * @return The cached {@link ColumnShard}s.
   */
  public Collection<ColumnShard> getAllCachedColumnShards(long firstRowIdInTableShard);
}
