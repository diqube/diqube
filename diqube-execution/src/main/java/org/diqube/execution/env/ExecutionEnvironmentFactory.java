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
package org.diqube.execution.env;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.TableShard;
import org.diqube.execution.cache.ColumnShardCacheRegistry;
import org.diqube.queries.QueryRegistry;

/**
 * Factory for {@link ExecutionEnvironment}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ExecutionEnvironmentFactory {

  @Inject
  private QueryRegistry queryRegistry;

  @Inject
  private ColumnShardCacheRegistry columnShardCacheRegistry;

  public ExecutionEnvironment createQueryMasterExecutionEnvironment() {
    return new DefaultExecutionEnvironment(queryRegistry, null, null);
  }

  public ExecutionEnvironment createQueryRemoteExecutionEnvironment(TableShard tableShard) {
    return new DefaultExecutionEnvironment(queryRegistry, tableShard,
        columnShardCacheRegistry.getOrCreateColumnShardCache(tableShard.getTableName()));
  }

  public DelegatingExecutionEnvironment createDelegatingExecutionEnvironment(ExecutionEnvironment delegate,
      int version) {
    return new DelegatingExecutionEnvironment(queryRegistry, delegate, version);
  }
}
