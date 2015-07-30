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
import java.util.Map;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
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
  protected LongColumnShard delegateGetLongColumnShard(String name) {
    if (tableShard != null)
      return tableShard.getLongColumns().get(name);
    return null;
  }

  @Override
  protected StringColumnShard delegateGetStringColumnShard(String name) {
    if (tableShard != null)
      return tableShard.getStringColumns().get(name);
    return null;
  }

  @Override
  protected DoubleColumnShard delegateGetDoubleColumnShard(String name) {
    if (tableShard != null)
      return tableShard.getDoubleColumns().get(name);
    return null;
  }

  @Override
  protected Map<String, ColumnShard> delegateGetAllColumnShards() {
    Map<String, ColumnShard> res = new HashMap<>();
    if (tableShard != null) {
      res.putAll(tableShard.getDoubleColumns());
      res.putAll(tableShard.getStringColumns());
      res.putAll(tableShard.getLongColumns());
    }

    return res;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[tableShard=" + ((tableShard == null) ? "null" : tableShard.toString())
        + "]";
  }

}
