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

import org.diqube.context.AutoInstatiate;

/**
 * Manages {@link WritableTableCache} instances.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableCacheRegistry {
  private ConcurrentMap<String, DefaultTableCache> caches = new ConcurrentHashMap<>();

  /**
   * @return The cache for the given table or <code>null</code> if not available.
   */
  public WritableTableCache getTableCache(String tableName) {
    return caches.get(tableName);
  }

  /**
   * @return The cache for the given table. If there was none, one is created.
   */
  public WritableTableCache getOrCreateTableCache(String tableName) {
    return caches.computeIfAbsent(tableName, s -> new DefaultTableCache());
  }
}
