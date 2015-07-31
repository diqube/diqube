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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.env.querystats.ColumnShardStatsFacade;
import org.diqube.execution.env.querystats.DoubleColumnShardStatsFacade;
import org.diqube.execution.env.querystats.LongColumnShardStatsFacade;
import org.diqube.execution.env.querystats.StringColumnShardStatsFacade;
import org.diqube.queries.QueryRegistry;

/**
 * Abstract implementation of an {@link ExecutionEnvironment} containing a separate set of Long, String and Double
 * columns.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractExecutionEnvironment implements ExecutionEnvironment {
  private Map<String, LongColumnShardStatsFacade> tempLongColumns = new ConcurrentHashMap<>();
  private Map<String, StringColumnShardStatsFacade> tempStringColumns = new ConcurrentHashMap<>();
  private Map<String, DoubleColumnShardStatsFacade> tempDoubleColumns = new ConcurrentHashMap<>();
  private QueryRegistry queryRegistry;

  public AbstractExecutionEnvironment(QueryRegistry queryRegistry) {
    this.queryRegistry = queryRegistry;
  }

  @Override
  public LongColumnShard getLongColumnShard(String name) {
    if (tempLongColumns.containsKey(name))
      return tempLongColumns.get(name);
    return delegateGetLongColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected LongColumnShard delegateGetLongColumnShard(String name);

  @Override
  public StringColumnShard getStringColumnShard(String name) {
    if (tempStringColumns.containsKey(name))
      return tempStringColumns.get(name);
    return delegateGetStringColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected StringColumnShard delegateGetStringColumnShard(String name);

  @Override
  public DoubleColumnShard getDoubleColumnShard(String name) {
    if (tempDoubleColumns.containsKey(name))
      return tempDoubleColumns.get(name);
    return delegateGetDoubleColumnShard(name);
  }

  /**
   * @return <code>null</code> if not available.
   */
  abstract protected DoubleColumnShard delegateGetDoubleColumnShard(String name);

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
  public ColumnShard getColumnShard(String name) {
    ColumnShard res = getLongColumnShard(name);
    if (res != null)
      return res;

    res = getStringColumnShard(name);
    if (res != null)
      return res;

    return getDoubleColumnShard(name);
  }

  @Override
  public void storeTemporaryLongColumnShard(LongColumnShard column) {
    queryRegistry.getOrCreateCurrentStats().incNumberOfTemporaryColumnsCreated();
    tempLongColumns.put(column.getName(), new LongColumnShardStatsFacade(column, true));
  }

  @Override
  public void storeTemporaryStringColumnShard(StringColumnShard column) {
    queryRegistry.getOrCreateCurrentStats().incNumberOfTemporaryColumnsCreated();
    tempStringColumns.put(column.getName(), new StringColumnShardStatsFacade(column, true));
  }

  @Override
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column) {
    queryRegistry.getOrCreateCurrentStats().incNumberOfTemporaryColumnsCreated();
    tempDoubleColumns.put(column.getName(), new DoubleColumnShardStatsFacade(column, true));
  }

  @Override
  public StandardColumnShard getPureStandardColumnShard(String name) {
    ColumnShard colShard = getColumnShard(name);

    if (colShard == null)
      return null;

    if (colShard instanceof ColumnShardStatsFacade)
      colShard = ((ColumnShardStatsFacade) colShard).getDelegate();

    if (colShard instanceof StandardColumnShard)
      return (StandardColumnShard) colShard;

    return null;
  }

  @Override
  public ConstantColumnShard getPureConstantColumnShard(String name) {
    ColumnShard colShard = getColumnShard(name);

    if (colShard == null)
      return null;

    if (colShard instanceof ColumnShardStatsFacade)
      colShard = ((ColumnShardStatsFacade) colShard).getDelegate();

    if (colShard instanceof ConstantColumnShard)
      return (ConstantColumnShard) colShard;

    return null;
  }

  @Override
  public Map<String, ColumnShard> getAllColumnShards() {
    Map<String, ColumnShard> res = new HashMap<>(delegateGetAllColumnShards());
    // temp cols stored in this EE override any delegate information.
    res.putAll(tempDoubleColumns);
    res.putAll(tempStringColumns);
    res.putAll(tempLongColumns);
    return res;
  }

  abstract protected Map<String, ColumnShard> delegateGetAllColumnShards();

  protected Set<String> getAllColumnNamesDefinedInThisEnv() {
    Set<String> res = new HashSet<>();
    res.addAll(tempDoubleColumns.keySet());
    res.addAll(tempLongColumns.keySet());
    res.addAll(tempStringColumns.keySet());
    return res;
  }

}
