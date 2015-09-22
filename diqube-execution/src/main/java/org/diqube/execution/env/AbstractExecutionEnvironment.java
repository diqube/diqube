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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.env.querystats.AbstractQueryableColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.execution.env.querystats.QueryableLongColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;
import org.diqube.execution.env.querystats.QueryableStringColumnShardFacade;
import org.diqube.queries.QueryRegistry;

import com.google.common.collect.Iterables;

/**
 * Abstract implementation of an {@link ExecutionEnvironment} containing a separate set of Long, String and Double
 * columns.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractExecutionEnvironment implements ExecutionEnvironment {
  private Map<String, QueryableLongColumnShardFacade> tempLongColumns = new ConcurrentHashMap<>();
  private Map<String, QueryableStringColumnShardFacade> tempStringColumns = new ConcurrentHashMap<>();
  private Map<String, QueryableDoubleColumnShardFacade> tempDoubleColumns = new ConcurrentHashMap<>();
  protected QueryRegistry queryRegistry;

  public AbstractExecutionEnvironment(QueryRegistry queryRegistry) {
    this.queryRegistry = queryRegistry;
  }

  @Override
  public QueryableLongColumnShard getLongColumnShard(String name) {
    if (tempLongColumns.containsKey(name))
      return tempLongColumns.get(name);
    return delegateGetLongColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected QueryableLongColumnShard delegateGetLongColumnShard(String name);

  @Override
  public QueryableStringColumnShard getStringColumnShard(String name) {
    if (tempStringColumns.containsKey(name))
      return tempStringColumns.get(name);
    return delegateGetStringColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected QueryableStringColumnShard delegateGetStringColumnShard(String name);

  @Override
  public QueryableDoubleColumnShard getDoubleColumnShard(String name) {
    if (tempDoubleColumns.containsKey(name))
      return tempDoubleColumns.get(name);
    return delegateGetDoubleColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected QueryableDoubleColumnShard delegateGetDoubleColumnShard(String name);

  @Override
  public ColumnType getColumnType(String colName) {
    if (getLongColumnShard(colName) != null)
      return ColumnType.LONG;
    if (getStringColumnShard(colName) != null)
      return ColumnType.STRING;
    if (getDoubleColumnShard(colName) != null)
      return ColumnType.DOUBLE;
    return null;
  }

  @Override
  public QueryableColumnShard getColumnShard(String name) {
    QueryableColumnShard res = getLongColumnShard(name);
    if (res != null)
      return res;

    res = getStringColumnShard(name);
    if (res != null)
      return res;

    return getDoubleColumnShard(name);
  }

  @Override
  public void storeTemporaryLongColumnShard(LongColumnShard column) {
    queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsCreated();
    internalStoreTemporaryLongColumnShard(column);
  }

  protected void internalStoreTemporaryLongColumnShard(LongColumnShard column) {
    tempLongColumns.put(column.getName(), new QueryableLongColumnShardFacade(column, true, queryRegistry));
  }

  @Override
  public void storeTemporaryStringColumnShard(StringColumnShard column) {
    queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsCreated();
    internalStoreTemporaryStringColumnShard(column);
  }

  protected void internalStoreTemporaryStringColumnShard(StringColumnShard column) {
    tempStringColumns.put(column.getName(), new QueryableStringColumnShardFacade(column, true, queryRegistry));
  }

  @Override
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column) {
    queryRegistry.getOrCreateCurrentStatsManager().incNumberOfTemporaryColumnShardsCreated();
    internalStoreTemporaryDoubleColumnShard(column);
  }

  protected void internalStoreTemporaryDoubleColumnShard(DoubleColumnShard column) {
    tempDoubleColumns.put(column.getName(), new QueryableDoubleColumnShardFacade(column, true, queryRegistry));
  }

  @Override
  public StandardColumnShard getPureStandardColumnShard(String name) {
    ColumnShard colShard = getColumnShard(name);

    if (colShard == null)
      return null;

    if (colShard instanceof QueryableColumnShard)
      colShard = ((QueryableColumnShard) colShard).getDelegate();

    if (colShard instanceof StandardColumnShard)
      return (StandardColumnShard) colShard;

    return null;
  }

  @Override
  public ConstantColumnShard getPureConstantColumnShard(String name) {
    ColumnShard colShard = getColumnShard(name);

    if (colShard == null)
      return null;

    if (colShard instanceof QueryableColumnShard)
      colShard = ((QueryableColumnShard) colShard).getDelegate();

    if (colShard instanceof ConstantColumnShard)
      return (ConstantColumnShard) colShard;

    return null;
  }

  protected Set<String> getAllColumnNamesDefinedInThisEnv() {
    Set<String> res = new HashSet<>();
    res.addAll(tempDoubleColumns.keySet());
    res.addAll(tempLongColumns.keySet());
    res.addAll(tempStringColumns.keySet());
    return res;
  }

  @Override
  public boolean isTemporaryColumn(String colName) {
    return tempDoubleColumns.containsKey(colName) || tempLongColumns.containsKey(colName)
        || tempStringColumns.containsKey(colName) || delegateIsTemporaryColumns(colName);
  }

  protected abstract boolean delegateIsTemporaryColumns(String colName);

  @Override
  public Map<String, List<QueryableColumnShard>> getAllTemporaryColumnShards() {
    Map<String, List<QueryableColumnShard>> res = delegateGetAllTemporaryColumnShards();
    for (Entry<String, ? extends AbstractQueryableColumnShardFacade> tempEntry : Iterables
        .concat(tempDoubleColumns.entrySet(), tempLongColumns.entrySet(), tempStringColumns.entrySet())) {
      if (!res.containsKey(tempEntry.getKey()))
        res.put(tempEntry.getKey(), new ArrayList<>());
      res.get(tempEntry.getKey()).add(tempEntry.getValue());
    }
    return res;
  }

  protected abstract Map<String, List<QueryableColumnShard>> delegateGetAllTemporaryColumnShards();

  @Override
  public Map<String, QueryableColumnShard> getAllNonTemporaryColumnShards() {
    return delegateGetAllNonTemporaryColumnShards();
  }

  protected abstract Map<String, QueryableColumnShard> delegateGetAllNonTemporaryColumnShards();

}
