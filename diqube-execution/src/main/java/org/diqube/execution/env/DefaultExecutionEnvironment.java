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
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.execution.env.querystats.QueryableLongColumnShardFacade;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;
import org.diqube.execution.env.querystats.QueryableStringColumnShardFacade;
import org.diqube.queries.QueryRegistry;

/**
 * Default implementation of a {@link ExecutionEnvironment} which is optionally based on the resources of a
 * {@link TableShard}.
 *
 * @author Bastian Gloeckle
 */
public class DefaultExecutionEnvironment extends AbstractExecutionEnvironment {
  /** could be <code>null</code> */
  private TableShard tableShard;

  /**
   * @param tableShard
   *          <code>null</code> for Query Master.
   */
  public DefaultExecutionEnvironment(QueryRegistry queryRegistry, TableShard tableShard) {
    super(queryRegistry);
    this.tableShard = tableShard;
  }

  @Override
  public TableShard getTableShardIfAvailable() {
    return tableShard;
  }

  @Override
  public long getFirstRowIdInShard() {
    if (tableShard != null)
      return tableShard.getLowestRowId();

    // execution on query master -> we're looking at the whole table here.
    return 0L;
  }

  @Override
  public long getLastRowIdInShard() {
    if (tableShard != null)
      return tableShard.getLowestRowId() + tableShard.getNumberOfRowsInShard() - 1;

    return -1;
  }

  @Override
  protected QueryableLongColumnShard delegateGetLongColumnShard(String name) {
    if (tableShard != null && tableShard.getLongColumns().get(name) != null)
      return new QueryableLongColumnShardFacade(tableShard.getLongColumns().get(name), false, queryRegistry);
    return null;
  }

  @Override
  protected QueryableStringColumnShard delegateGetStringColumnShard(String name) {
    if (tableShard != null && tableShard.getStringColumns().get(name) != null)
      return new QueryableStringColumnShardFacade(tableShard.getStringColumns().get(name), false, queryRegistry);
    return null;
  }

  @Override
  protected QueryableDoubleColumnShard delegateGetDoubleColumnShard(String name) {
    if (tableShard != null && tableShard.getDoubleColumns().get(name) != null)
      return new QueryableDoubleColumnShardFacade(tableShard.getDoubleColumns().get(name), false, queryRegistry);
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

  @Override
  public void storeTemporaryLongColumnShard(LongColumnShard column) {
    if (getColumnShard(column.getName()) != null)
      throw new IllegalArgumentException("Column " + column.getName() + " already exists.");
    super.storeTemporaryLongColumnShard(column);
  }

  @Override
  public void storeTemporaryStringColumnShard(StringColumnShard column) {
    if (getColumnShard(column.getName()) != null)
      throw new IllegalArgumentException("Column " + column.getName() + " already exists.");
    super.storeTemporaryStringColumnShard(column);
  }

  @Override
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column) {
    if (getColumnShard(column.getName()) != null)
      throw new IllegalArgumentException("Column " + column.getName() + " already exists.");
    super.storeTemporaryDoubleColumnShard(column);
  }

}
