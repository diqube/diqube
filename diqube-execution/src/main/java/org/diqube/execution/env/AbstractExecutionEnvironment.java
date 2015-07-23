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
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;

/**
 * Abstract implementation of an {@link ExecutionEnvironment} containing a separate set of Long, String and Double
 * columns.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractExecutionEnvironment implements ExecutionEnvironment {
  private Map<String, LongColumnShard> tempLongColumns = new ConcurrentHashMap<>();
  private Map<String, StringColumnShard> tempStringColumns = new ConcurrentHashMap<>();
  private Map<String, DoubleColumnShard> tempDoubleColumns = new ConcurrentHashMap<>();

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
    tempLongColumns.put(column.getName(), column);
  }

  @Override
  public void storeTemporaryStringColumnShard(StringColumnShard column) {
    tempStringColumns.put(column.getName(), column);
  }

  @Override
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column) {
    tempDoubleColumns.put(column.getName(), column);
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
