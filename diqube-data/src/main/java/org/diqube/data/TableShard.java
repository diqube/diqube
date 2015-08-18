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
package org.diqube.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.DoubleStandardColumnShard;
import org.diqube.data.lng.LongStandardColumnShard;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnShard;
import org.diqube.data.serialize.thrift.v1.STableShard;
import org.diqube.data.str.StringStandardColumnShard;

import com.google.common.collect.Iterables;

/**
 * A {@link TableShard} contains all data of a specific consecutive subset of rows of a {@link Table}.
 * 
 * <p>
 * It is made up of various {@link ColumnShard}s each of one of the core data types: String, Long, Double.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = STableShard.class)
public class TableShard implements DataSerialization<STableShard> {
  private Map<String, StringStandardColumnShard> stringColumns = new HashMap<>();
  private Map<String, DoubleStandardColumnShard> doubleColumns = new HashMap<>();
  private Map<String, LongStandardColumnShard> longColumns = new HashMap<>();

  private String tableName;

  /** for deserialization only */
  public TableShard() {
  }

  /* package */ TableShard(String tableName, Collection<StandardColumnShard> columns) {
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

  public Map<String, StringStandardColumnShard> getStringColumns() {
    return stringColumns;
  }

  public Map<String, DoubleStandardColumnShard> getDoubleColumns() {
    return doubleColumns;
  }

  public Map<String, LongStandardColumnShard> getLongColumns() {
    return longColumns;
  }

  public Map<String, StandardColumnShard> getColumns() {
    Map<String, StandardColumnShard> res = new HashMap<>();
    res.putAll(stringColumns);
    res.putAll(longColumns);
    res.putAll(doubleColumns);

    return res;
  }

  /**
   * @return The number of rows contained in this shard.
   */
  public long getNumberOfRowsInShard() {
    if (stringColumns.size() > 0)
      return stringColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    if (doubleColumns.size() > 0)
      return doubleColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    if (longColumns.size() > 0)
      return longColumns.values().iterator().next().getNumberOfRowsInColumnShard();
    return 0;
  }

  /**
   * @return The lowest rowId stored in this TableShard. -1 if {@link #getNumberOfRowsInShard()} == 0.
   */
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
}
