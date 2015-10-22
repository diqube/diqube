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

import org.diqube.data.column.ColumnShard;

/**
 * A cache that does not cache anything. Will be implemented in case the user configuration disables caching.
 *
 * @author Bastian Gloeckle
 */
public class NoopColumnShardCache implements WritableColumnShardCache {

  @Override
  public ColumnShard getCachedColumnShard(long firstRowIdTableShard, String colName) {
    return null;
  }

  @Override
  public Collection<ColumnShard> getAllCachedColumnShards(long firstRowIdInTableShard) {
    return new ArrayList<>();
  }

  @Override
  public void registerUsageOfColumnShardPossiblyCache(long firstRowIdInTableShard, ColumnShard createdColumnShard) {
    // noop.
  }

  @Override
  public int getNumberOfColumnShardsCached() {
    return 0;
  }

}
