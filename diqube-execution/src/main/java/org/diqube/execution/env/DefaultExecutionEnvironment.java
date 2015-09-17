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
import java.util.List;
import java.util.Map;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.cache.TableCache;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.execution.env.querystats.QueryableLongColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;
import org.diqube.execution.env.querystats.QueryableStringColumnShardFacade;
import org.diqube.queries.QueryRegistry;

/**
 * Default implementation of a {@link ExecutionEnvironment} which is based on the resources of a {@link TableShard} if
 * it is used on a query remote.
 * 
 * <p>
 * This implementation is {@link TableCache}-aware. This means that it will provide columns that are stored in the
 * {@link TableCache} for the corresponding {@link TableShard}.
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
  private TableCache tableCache;

  /**
   * @param tableShard
   *          <code>null</code> for Query Master.
   * @param tableCache
   *          The cache to read ColumnShards from.
   */
  public DefaultExecutionEnvironment(QueryRegistry queryRegistry, TableShard tableShard, TableCache tableCache) {
    super(queryRegistry);
    this.tableShard = tableShard;
    this.tableCache = tableCache;

    if (tableCache != null && tableShard == null)
      throw new IllegalArgumentException();

    if (tableCache != null)
      for (ColumnShard cs : tableCache.getAllCachedColumnShards(tableShard.getLowestRowId())) {
        if (cs instanceof StringColumnShard)
          storeTemporaryStringColumnShard((StringColumnShard) cs);
        else if (cs instanceof LongColumnShard)
          storeTemporaryLongColumnShard((LongColumnShard) cs);
        else if (cs instanceof DoubleColumnShard)
          storeTemporaryDoubleColumnShard((DoubleColumnShard) cs);
      }
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

    if (sourceColumnShard == null && tableCache != null) {
      ColumnShard cachedShard = tableCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof LongColumnShard) {
        sourceColumnShard = (LongColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        storeTemporaryLongColumnShard(sourceColumnShard);
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

    if (sourceColumnShard == null && tableCache != null) {
      ColumnShard cachedShard = tableCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof StringColumnShard) {
        sourceColumnShard = (StringColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        storeTemporaryStringColumnShard(sourceColumnShard);
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

    if (sourceColumnShard == null && tableCache != null) {
      ColumnShard cachedShard = tableCache.getCachedColumnShard(tableShard.getLowestRowId(), name);
      if (cachedShard != null && cachedShard instanceof DoubleColumnShard) {
        sourceColumnShard = (DoubleColumnShard) cachedShard;
        // store col shard directly in our "temporary columns". see class comment.
        storeTemporaryDoubleColumnShard(sourceColumnShard);
      }
    }

    if (sourceColumnShard != null)
      return new QueryableDoubleColumnShardFacade(sourceColumnShard, false, queryRegistry);
    return null;
  }

  @Override
  protected Map<String, QueryableColumnShard> delegateGetAllColumnShards() {
    Map<String, QueryableColumnShard> res = new HashMap<>();
    if (tableShard != null) {
      tableShard.getDoubleColumns().entrySet().stream().forEach(entry -> res.put(entry.getKey(),
          new QueryableDoubleColumnShardFacade(entry.getValue(), false, queryRegistry)));

      tableShard.getStringColumns().entrySet().stream().forEach(entry -> res.put(entry.getKey(),
          new QueryableStringColumnShardFacade(entry.getValue(), false, queryRegistry)));

      tableShard.getLongColumns().entrySet().stream().forEach(
          entry -> res.put(entry.getKey(), new QueryableLongColumnShardFacade(entry.getValue(), false, queryRegistry)));
    }

    if (tableCache != null) {
      for (ColumnShard cachedShard : tableCache.getAllCachedColumnShards(tableShard.getLowestRowId())) {
        if (cachedShard instanceof StringColumnShard)
          res.put(cachedShard.getName(),
              new QueryableStringColumnShardFacade((StringColumnShard) cachedShard, true, queryRegistry));
        else if (cachedShard instanceof LongColumnShard)
          res.put(cachedShard.getName(),
              new QueryableLongColumnShardFacade((LongColumnShard) cachedShard, true, queryRegistry));
        else if (cachedShard instanceof DoubleColumnShard)
          res.put(cachedShard.getName(),
              new QueryableDoubleColumnShardFacade((DoubleColumnShard) cachedShard, true, queryRegistry));
      }
    }

    return res;
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
    if (tableShard != null)
      res.putAll(delegateGetAllColumnShards());

    return res;
  }

}
