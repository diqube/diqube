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
package org.diqube.data.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnShard;
import org.diqube.data.serialize.thrift.v1.STableShard;
import org.diqube.data.types.dbl.DoubleStandardColumnShard;
import org.diqube.data.types.lng.LongStandardColumnShard;
import org.diqube.data.types.str.StringStandardColumnShard;

import com.google.common.collect.Iterables;

/**
 * Default implementation of {@link TableShard} for regular tables.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = STableShard.class)
public class DefaultTableShard implements TableShard {
  private Map<String, StringStandardColumnShard> stringColumns = new HashMap<>();
  private Map<String, DoubleStandardColumnShard> doubleColumns = new HashMap<>();
  private Map<String, LongStandardColumnShard> longColumns = new HashMap<>();

  private String tableName;

  /** for deserialization only */
  public DefaultTableShard() {
  }

  protected DefaultTableShard(String tableName, Collection<StandardColumnShard> columns) {
    this.tableName = tableName;
    for (ColumnShard col : columns) {
      switch (col.getColumnType()) {
      case STRING:
        stringColumns.put(col.getName(), (StringStandardColumnShard) col);
        break;
      case LONG:
        longColumns.put(col.getName(), (LongStandardColumnShard) col);
        break;
      case DOUBLE:
        doubleColumns.put(col.getName(), (DoubleStandardColumnShard) col);
        break;
      }
    }
  }

  @Override
  public Map<String, StringStandardColumnShard> getStringColumns() {
    return stringColumns;
  }

  @Override
  public Map<String, DoubleStandardColumnShard> getDoubleColumns() {
    return doubleColumns;
  }

  @Override
  public Map<String, LongStandardColumnShard> getLongColumns() {
    return longColumns;
  }

  @Override
  public Map<String, StandardColumnShard> getColumns() {
    Map<String, StandardColumnShard> res = new HashMap<>();
    res.putAll(stringColumns);
    res.putAll(longColumns);
    res.putAll(doubleColumns);

    return res;
  }

  @Override
  public long getNumberOfRowsInShard() {
    if (stringColumns.size() > 0)
      return stringColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    if (doubleColumns.size() > 0)
      return doubleColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    if (longColumns.size() > 0)
      return longColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    return 0;
  }

  @Override
  public long getLowestRowId() {
    if (stringColumns.size() > 0)
      return stringColumns.values().iterator().next().getPages().firstKey();
    if (doubleColumns.size() > 0)
      return doubleColumns.values().iterator().next().getPages().firstKey();
    if (longColumns.size() > 0)
      return longColumns.values().iterator().next().getPages().firstKey();
    return -1;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, STableShard target) throws SerializationException {
    target.setTableName(tableName);
    List<SColumnShard> serializedCols = new ArrayList<>();
    for (StandardColumnShard shard : Iterables.concat(stringColumns.values(), longColumns.values(),
        doubleColumns.values()))
      serializedCols.add(mgr.serializeChild(SColumnShard.class, shard));
    target.setColumnShards(serializedCols);
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, STableShard source) throws DeserializationException {
    this.tableName = source.getTableName();
    for (SColumnShard serCol : source.getColumnShards()) {
      StandardColumnShard de = mgr.deserializeChild(StandardColumnShard.class, serCol);
      if (de instanceof StringStandardColumnShard)
        stringColumns.put(de.getName(), (StringStandardColumnShard) de);
      else if (de instanceof LongStandardColumnShard)
        longColumns.put(de.getName(), (LongStandardColumnShard) de);
      else if (de instanceof DoubleStandardColumnShard)
        doubleColumns.put(de.getName(), (DoubleStandardColumnShard) de);
      else
        throw new DeserializationException("Cannot deserialize column " + de.getName());
    }
  }

  /** Only allowed to be called before the TableShard is registered in the TableRegistry! */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    long sumColNameLen = 0L;
    long colSizes = 0L;
    for (Entry<String, StringStandardColumnShard> shardEntry : stringColumns.entrySet()) {
      sumColNameLen += shardEntry.getKey().length();
      colSizes += shardEntry.getValue().calculateApproximateSizeInBytes();
    }
    for (Entry<String, DoubleStandardColumnShard> shardEntry : doubleColumns.entrySet()) {
      sumColNameLen += shardEntry.getKey().length();
      colSizes += shardEntry.getValue().calculateApproximateSizeInBytes();
    }
    for (Entry<String, LongStandardColumnShard> shardEntry : longColumns.entrySet()) {
      sumColNameLen += shardEntry.getKey().length();
      colSizes += shardEntry.getValue().calculateApproximateSizeInBytes();
    }

    return 16 + //
        tableName.length() + //
        colSizes + //
        sumColNameLen;
  }
}
