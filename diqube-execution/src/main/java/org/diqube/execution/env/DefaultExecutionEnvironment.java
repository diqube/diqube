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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.cache.ColumnShardCache;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.execution.env.querystats.QueryableLongColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;
import org.diqube.execution.env.querystats.QueryableStringColumnShardFacade;
import org.diqube.queries.QueryRegistry;

import com.google.common.collect.Sets;

/**
 * Default implementation of a {@link ExecutionEnvironment} which is based on the resources of a {@link TableShard} if
 * it is used on a query remote.
 * 
 * <p>
 * This implementation is {@link ColumnShardCache}-aware. This means that it will provide columns that are stored in the
 * {@link ColumnShardCache} for the corresponding {@link TableShard}.
 * 
 * If a {@link ColumnShard} is found to be cached, it is put directly into this {@link DefaultExecutionEnvironment} as
 * "temporary" column (just like when somebody calls {@link #storeTemporaryDoubleColumnShard(DoubleColumnShard)} etc).
 * This is because (1) the ColumnShard might be evicted from the cache while we're executing the plan - but the plan
 * relies on a column that was available once to be available in the future. And (2) it makes it easier to identify the
 * (new?) columns that should be cached after executing the plan this {@link DefaultExecutionEnvironment} belongs to.
 *
 * @author Bastian Gloeckle
 */
public class DefaultExecutionEnvironment extends AbstractExecutionEnvironment {
  /** could be <code>null</code> */
  private TableShard tableShard;
  private ColumnShardCache columnShardCache;

  /**
   * @param tableShard
   *          <code>null</code> for Query Master.
   * @param columnShardCache
   *          The cache to read ColumnShards from. This needs to be that cache that is responsible for the given
   *          tableShard. Cannot be set if parameter tableShard == null. Can be <code>null</code>.
   */
  public DefaultExecutionEnvironment(QueryRegistry queryRegistry, TableShard tableShard,
      ColumnShardCache columnShardCache) {
    super(queryRegistry);
    this.tableShard = tableShard;
    this.columnShardCache = columnShardCache;

    if (columnShardCache != null && tableShard == null)
      throw new IllegalArgumentException();
  }

  @Override
  public long getFirstRowIdInShard() {
    if (tableShard != null)
      return tableShard.getLowestRowId();

    // execution on query master -> we're looking at the whole table here.
    return 0L;
  }

  @Override
  public long getNumberOfRowsInShard() {
    if (tableShard != null)
      return tableShard.getNumberOfRowsInShard();
    return -1L;
  }

  @Override
  public long getLastRowIdInShard() {
    if (tableShard != null)
      return tableShard.getLowestRowId() + tableShard.getNumberOfRowsInShard() - 1;

    return -1L;
  }

  @Override
  protected QueryableLongColumnShard delegateGetLongColumnShard(String name) {
    LongColumnShard sourceColumnShard = null;
    if (tableShard != null)
      sourceColumnShard = tableShard.getLongColumns().get(name);

    if (sourceColumnShard == null && columnShardCache != null) {
      ColumnShard cachedShard = columnShardCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof LongColumnShard) {
        sourceColumnShard = (LongColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsFromCache();
        internalStoreTemporaryLongColumnShard(sourceColumnShard);
      }
    }

    if (sourceColumnShard != null)
      return new QueryableLongColumnShardFacade(sourceColumnShard, false, queryRegistry);
    return null;
  }

  @Override
  protected QueryableStringColumnShard delegateGetStringColumnShard(String name) {
    StringColumnShard sourceColumnShard = null;
    if (tableShard != null)
      sourceColumnShard = tableShard.getStringColumns().get(name);

    if (sourceColumnShard == null && columnShardCache != null) {
      ColumnShard cachedShard = columnShardCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof StringColumnShard) {
        sourceColumnShard = (StringColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsFromCache();
        internalStoreTemporaryStringColumnShard(sourceColumnShard);
      }
    }

    if (sourceColumnShard != null)
      return new QueryableStringColumnShardFacade(sourceColumnShard, false, queryRegistry);
    return null;
  }

  @Override
  protected QueryableDoubleColumnShard delegateGetDoubleColumnShard(String name) {
    DoubleColumnShard sourceColumnShard = null;
    if (tableShard != null)
      sourceColumnShard = tableShard.getDoubleColumns().get(name);

    if (sourceColumnShard == null && columnShardCache != null) {
      ColumnShard cachedShard = columnShardCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof DoubleColumnShard) {
        sourceColumnShard = (DoubleColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsFromCache();
        internalStoreTemporaryDoubleColumnShard(sourceColumnShard);
      }
    }

    if (sourceColumnShard != null)
      return new QueryableDoubleColumnShardFacade(sourceColumnShard, false, queryRegistry);
    return null;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[tableShard=" + ((tableShard == null) ? "null" : tableShard.toString())
        + "]";
  }

  @Override
  protected boolean delegateIsTemporaryColumns(String colName) {
    // delegate loads from tableShard, so "no", this col is no temp col.
    return false;
  }

  @Override
  protected Map<String, List<QueryableColumnShard>> delegateGetAllTemporaryColumnShards() {
    return new HashMap<>();
  }

  @Override
  protected Map<String, QueryableColumnShard> delegateGetAllNonTemporaryColumnShards() {
    Map<String, QueryableColumnShard> res = new HashMap<>();

    Set<String> allColNames = new HashSet<>();

    if (tableShard != null) {
      allColNames
          .addAll(Sets.union(Sets.union(tableShard.getDoubleColumns().keySet(), tableShard.getLongColumns().keySet()),
              tableShard.getStringColumns().keySet()));
    }

    for (String colName : allColNames) {
      QueryableColumnShard colShard = getColumnShard(colName);
      res.put(colName, colShard);
    }

    return res;
  }

}
