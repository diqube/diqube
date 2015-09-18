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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;

/**
 * Manages {@link WritableColumnShardCache} instances.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ColumnShardCacheRegistry {
  private ConcurrentMap<String, WritableColumnShardCache> caches = new ConcurrentHashMap<>();

  @Config(ConfigKey.TABLE_CACHE_APPROX_MAX_PER_TABLE_MB)
  public int tableCacheApproxMaxPerTableMb;

  /**
   * @return The cache for the given table or <code>null</code> if not available.
   */
  public WritableColumnShardCache getColumnShardCache(String tableName) {
    return caches.get(tableName);
  }

  /**
   * @return The cache for the given table. If there was none, one is created.
   */
  public WritableColumnShardCache getOrCreateColumnShardCache(String tableName) {
    if (tableCacheApproxMaxPerTableMb <= 0)
      return caches.computeIfAbsent(tableName, s -> new NoopColumnShardCache());
    else
      return caches.computeIfAbsent(tableName,
          s -> new DefaultColumnShardCache(tableCacheApproxMaxPerTableMb * 1024L * 1024L));
  }
}
