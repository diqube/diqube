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

import org.diqube.data.column.ColumnShard;
import org.diqube.data.table.TableShard;
import org.diqube.loader.columnshard.SparseColumnShardBuilder;

/**
 * A Column Shard cache caches column shards for a single table, this interface allows updating the cache.
 * 
 * @author Bastian Gloeckle
 */
public interface WritableColumnShardCache extends ColumnShardCache {

  /**
   * Register that a specific {@link ColumnShard} has been created/used once and offer the cache to cache that column
   * shard at the discretion of the cache.
   * 
   * @param firstRowIdInTableShard
   *          {@link TableShard#getLowestRowId()} of the shard the given col was created for.
   * @param createdColumnShard
   *          The column shard that has been created and should perhaps be cached. Note that this must not be a
   *          {@link ColumnShard} that has been created using the {@link SparseColumnShardBuilder}, as such a col shard
   *          cannot be used by anyone else.
   */
  public void registerUsageOfColumnShardPossiblyCache(long firstRowIdInTableShard, ColumnShard createdColumnShard);
}
