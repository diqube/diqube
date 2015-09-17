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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.diqube.data.Table;
import org.diqube.data.colshard.ColumnShard;

/**
 * Default implementation of a cache that caches data in the context of a {@link Table}.
 *
 * @author Bastian Gloeckle
 */
public class DefaultTableCache implements WritableTableCache {
  private ConcurrentMap<Long, ConcurrentMap<String, ColumnShard>> allCachesByTableShard = new ConcurrentHashMap<>();

  @Override
  public ColumnShard getCachedColumnShard(long firstRowIdTableShard, String colName) {
    ConcurrentMap<String, ColumnShard> cache = allCachesByTableShard.get(firstRowIdTableShard);
    if (cache == null)
      return null;

    return cache.get(colName);
  }

  @Override
  public Collection<ColumnShard> getAllCachedColumnShards(long firstRowIdInTableShard) {
    ConcurrentMap<String, ColumnShard> cache = allCachesByTableShard.get(firstRowIdInTableShard);
    if (cache == null)
      return new ArrayList<>();
    return new ArrayList<>(cache.values());
  }
}
