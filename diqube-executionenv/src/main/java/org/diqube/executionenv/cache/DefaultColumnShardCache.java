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
package org.diqube.executionenv.cache;

import org.diqube.cache.CountingCache;
import org.diqube.data.column.ColumnShard;

/**
 * Counting cache for ColumnShards of a single table.
 * 
 * @author Bastian Gloeckle
 */
public class DefaultColumnShardCache extends CountingCache<Long, String, ColumnShard>
    implements WritableColumnShardCache {

  protected DefaultColumnShardCache(long maxMemoryBytes) {
    super(maxMemoryBytes, (colShard) -> colShard.calculateApproximateSizeInBytes());
  }

  @Override
  protected long getMaxMemoryBytes() {
    return super.getMaxMemoryBytes();
  }

  @Override
  protected void removeFromCache(Long key1, String key2) {
    super.removeFromCache(key1, key2);
  }

}
