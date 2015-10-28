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
package org.diqube.data.flatten;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnShard;
import org.diqube.data.table.Table;

/**
 * Abstract base class for flattened column shards.
 * 
 * <p>
 * Flattened column shards are created for a {@link Table} when that table is flattened by a specific (repeated) field.
 * The result is a table hat has different columns with partially repeated data compared to the columns in the original
 * table. This column shard contains basically the whole implementation needed for a flattened column shard. It does not
 * hold any data itself, but uses the {@link ColumnShard}s of the original {@link Table} as delegates.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractFlattenedStandardColumnShard implements StandardColumnShard {

  private String name;
  private ColumnType colType;
  private Dictionary<?> columnShardDict;
  private long firstRowId;
  private NavigableMap<Long, ColumnPage> pages;

  protected AbstractFlattenedStandardColumnShard(String name, ColumnType colType, Dictionary<?> columnShardDict,
      long firstRowId, List<ColumnPage> pages) {
    this.name = name;
    this.colType = colType;
    this.columnShardDict = columnShardDict;
    this.firstRowId = firstRowId;
    this.pages = new TreeMap<>();
    for (ColumnPage page : pages) {
      this.pages.put(page.getFirstRowId(), page);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Dictionary<?> getColumnShardDictionary() {
    return columnShardDict;
  }

  @Override
  public ColumnType getColumnType() {
    return colType;
  }

  @Override
  public long getFirstRowId() {
    return firstRowId;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 0;
  }

  @Override
  public NavigableMap<Long, ColumnPage> getPages() {
    return pages;
  }

  @Override
  public long getNumberOfRowsInColumnShard() {
    return pages.values().stream().mapToLong(page -> page.getValues().size()).sum();
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, SColumnShard target)
      throws SerializationException {
    throw new SerializationException("Cannot serialize a flattened ColShard.");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      SColumnShard source) throws DeserializationException {
    throw new DeserializationException("Cannot deserialize a flattened ColShard.");
  }

}
