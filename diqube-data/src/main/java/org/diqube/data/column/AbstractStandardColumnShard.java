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
package org.diqube.data.column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.dictionary.SerializableDictionary;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.serialize.thrift.v1.SColumnShard;
import org.diqube.data.serialize.thrift.v1.SColumnType;
import org.diqube.data.serialize.thrift.v1.SDictionary;

/**
 * Abstract implementation of a {@link StandardColumnShard}.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractStandardColumnShard implements StandardColumnShard, AdjustableStandardColumnShard {
  protected SerializableDictionary<?, ?> columnShardDictionary;

  protected String name;

  protected ColumnType columnType;

  /**
   * Map from firstId to {@link ColumnPage}
   */
  protected NavigableMap<Long, ColumnPage> pages;

  /** for deserialization */
  protected AbstractStandardColumnShard() {

  }

  /**
   * Create a new column shard.
   * 
   * @param columnType
   *          Type of the column
   * @param name
   *          Name of the column
   * @param pages
   *          ColumnPages, mapping from first RowID to the page itself. This map object can be empty when creating this
   *          ColumnShard and be filled later on.
   * @param columnShardDictionary
   *          The Column dictionary, see class comment.
   */
  protected AbstractStandardColumnShard(ColumnType columnType, String name, NavigableMap<Long, ColumnPage> pages,
      SerializableDictionary<?, ?> columnShardDictionary) {
    this.columnType = columnType;
    this.name = name;
    this.pages = pages;
    this.columnShardDictionary = columnShardDictionary;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public NavigableMap<Long, ColumnPage> getPages() {
    return pages;
  }

  @Override
  public Dictionary<?> getColumnShardDictionary() {
    return columnShardDictionary;
  }

  @Override
  public ColumnType getColumnType() {
    return columnType;
  }

  @Override
  public long getNumberOfRowsInColumnShard() {
    if (pages.size() == 0)
      return 0;

    return pages.values().stream().mapToLong(page -> page.size()).sum();
  }

  @Override
  public long getFirstRowId() {
    return pages.firstKey();
  }

  @Override
  public void adjustToFirstRowId(long firstRowId) throws UnsupportedOperationException {
    long delta = firstRowId - pages.firstKey();

    if (pages.values().stream().anyMatch(page -> !(page instanceof AdjustableColumnPage)))
      throw new UnsupportedOperationException("Cannot adjust rowIDs, because not all ColumnPages are adjustable.");

    NavigableMap<Long, ColumnPage> newPages = new TreeMap<>();
    for (Entry<Long, ColumnPage> pageEntry : pages.entrySet()) {
      ColumnPage page = pageEntry.getValue();
      ((AdjustableColumnPage) page).setFirstRowId(page.getFirstRowId() + delta);
      newPages.put(pageEntry.getKey() + delta, page);
    }

    pages = newPages;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SColumnShard target) throws SerializationException {
    target.setName(name);
    switch (columnType) {
    case STRING:
      target.setType(SColumnType.STRING);
      break;
    case LONG:
      target.setType(SColumnType.LONG);
      break;
    case DOUBLE:
      target.setType(SColumnType.DOUBLE);
      break;
    }
    target.setDictionary(mgr.serializeChild(SDictionary.class, columnShardDictionary));
    List<SColumnPage> serializedPages = new ArrayList<>();
    for (ColumnPage page : pages.values())
      serializedPages.add(mgr.serializeChild(SColumnPage.class, page));
    target.setPages(serializedPages);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SColumnShard source) throws DeserializationException {
    name = source.getName();
    switch (source.getType()) {
    case STRING:
      columnType = ColumnType.STRING;
      break;
    case LONG:
      columnType = ColumnType.LONG;
      break;
    case DOUBLE:
      columnType = ColumnType.DOUBLE;
      break;
    default:
      throw new DeserializationException("Unknown type");
    }
    columnShardDictionary = mgr.deserializeChild(SerializableDictionary.class, source.getDictionary());
    pages = new TreeMap<>();
    for (SColumnPage serializedPage : source.getPages()) {
      ColumnPage page = mgr.deserializeChild(DefaultColumnPage.class, serializedPage);
      pages.put(page.getFirstRowId(), page);
    }
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    long pagesSize = 0L;
    // "Long" keys: 8 bytes long value, 16 bytes object header
    pagesSize += pages.size() * (8 + 16);
    // ColumnPages:
    for (ColumnPage page : pages.values()) {
      pagesSize += page.calculateApproximateSizeInBytes();
    }
    return 16 + // object header of this.
        name.getBytes().length + //
        columnShardDictionary.calculateApproximateSizeInBytes() + pagesSize;
  }

}
