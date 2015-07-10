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
package org.diqube.loader.columnshard;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnPageFactory;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.dict.DoubleDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.data.str.dict.StringDictionary;
import org.diqube.loader.compression.CompressedDoubleDictionaryBuilder;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.loader.compression.CompressedStringDictionaryBuilder;
import org.diqube.util.Pair;

/**
 * A {@link SparseColumnShardBuilder} builds a {@link StandardColumnShard} of which it is expected to have only a sparse
 * set of values for the rows.
 *
 * TODO check if separate implementation of Columnshard should be used for this.
 *
 * @author Bastian Gloeckle
 */
public class SparseColumnShardBuilder<T> {
  private ConcurrentHashMap<Long, T> rowIdToValues = new ConcurrentHashMap<>();
  private ColumnShardFactory columnShardFactory;
  private ColumnPageFactory columnPageFactory;
  private String name;
  private long numberOfRows;

  public SparseColumnShardBuilder(ColumnShardFactory columnShardFactory, ColumnPageFactory columnPageFactory,
      String name) {
    this.columnShardFactory = columnShardFactory;
    this.columnPageFactory = columnPageFactory;
    this.name = name;
  }

  public SparseColumnShardBuilder<T> withValues(Map<Long, T> values) {
    rowIdToValues.putAll(values);
    return this;
  }

  public SparseColumnShardBuilder<T> withNumberOfRows(long numberOfRows) {
    this.numberOfRows = numberOfRows;
    return this;
  }

  public ColumnShard build() {
    NavigableMap<Long, Long> navigableRowIdToValueIds = new TreeMap<Long, Long>();

    T sampleValue = rowIdToValues.values().iterator().next();
    Class<?> columnValueClass = sampleValue.getClass();

    ColumnShard res = null;
    NavigableMap<Long, ColumnPage> pages = new TreeMap<>();

    Map<Long, Long> idChangeMap = null;

    Long defaultValueId = null;

    if (columnValueClass.equals(Long.class)) {
      Long defaultValue = 0L;

      NavigableMap<Long, Long> entityMap = new TreeMap<>();
      long tmpValueId = 0;
      for (Entry<Long, T> valueEntry : rowIdToValues.entrySet()) {
        if (!entityMap.containsKey(valueEntry.getValue()))
          entityMap.put((Long) valueEntry.getValue(), tmpValueId++);

        navigableRowIdToValueIds.put(valueEntry.getKey(), entityMap.get(valueEntry.getValue()));
      }

      if (!entityMap.containsKey(defaultValue))
        entityMap.put(defaultValue, tmpValueId++);

      defaultValueId = entityMap.get(defaultValue);

      CompressedLongDictionaryBuilder builder = new CompressedLongDictionaryBuilder();
      builder.withDictionaryName(name).fromEntityMap(entityMap);
      Pair<LongDictionary, Map<Long, Long>> builderRes = builder.build();
      LongDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardLongColumnShard(name, pages, columnShardDictionary);
    } else if (columnValueClass.equals(String.class)) {
      // TODO optimize c&p
      String defaultValue = "";

      NavigableMap<String, Long> entityMap = new TreeMap<>();
      long tmpValueId = 0;
      for (Entry<Long, T> valueEntry : rowIdToValues.entrySet()) {
        if (!entityMap.containsKey(valueEntry.getValue()))
          entityMap.put((String) valueEntry.getValue(), tmpValueId++);

        navigableRowIdToValueIds.put(valueEntry.getKey(), entityMap.get(valueEntry.getValue()));
      }

      if (!entityMap.containsKey(defaultValue))
        entityMap.put(defaultValue, tmpValueId++);

      defaultValueId = entityMap.get(defaultValue);

      CompressedStringDictionaryBuilder builder = new CompressedStringDictionaryBuilder();
      builder.fromEntityMap(entityMap);
      Pair<StringDictionary, Map<Long, Long>> builderRes = builder.build();
      StringDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardStringColumnShard(name, pages, columnShardDictionary);
    } else if (columnValueClass.equals(Double.class)) {
      // TODO optimize c&p
      Double defaultValue = -1.;

      NavigableMap<Double, Long> entityMap = new TreeMap<>();
      long tmpValueId = 0;
      for (Entry<Long, T> valueEntry : rowIdToValues.entrySet()) {
        if (!entityMap.containsKey(valueEntry.getValue()))
          entityMap.put((Double) valueEntry.getValue(), tmpValueId++);

        navigableRowIdToValueIds.put(valueEntry.getKey(), entityMap.get(valueEntry.getValue()));
      }

      if (!entityMap.containsKey(defaultValue))
        entityMap.put(defaultValue, tmpValueId++);

      defaultValueId = entityMap.get(defaultValue);

      CompressedDoubleDictionaryBuilder builder = new CompressedDoubleDictionaryBuilder();
      builder.fromEntityMap(entityMap);
      Pair<DoubleDictionary, Map<Long, Long>> builderRes = builder.build();
      DoubleDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardDoubleColumnShard(name, pages, columnShardDictionary);
    } else
      throw new RuntimeException("Cannot build sparse column of type " + columnValueClass); // should not happen

    long maxRowId = navigableRowIdToValueIds.lastKey();
    numberOfRows = Long.max(numberOfRows, maxRowId + 1);

    int numberOfPages = (int) (numberOfRows / ColumnShardBuilder.PROPOSAL_ROWS);
    if (numberOfRows % ColumnShardBuilder.PROPOSAL_ROWS != 0)
      numberOfPages++;

    Iterator<Entry<Long, Long>> navigableRowIdToValuesIterator = navigableRowIdToValueIds.entrySet().iterator();
    Entry<Long, Long> nextRowIdToValue = null;
    for (int pageNo = 0; pageNo < numberOfPages; pageNo++) {
      NavigableMap<Long, Long> valueToId = new TreeMap<>();

      int valueLength = ColumnShardBuilder.PROPOSAL_ROWS;
      if (pageNo == numberOfPages - 1 && numberOfRows % ColumnShardBuilder.PROPOSAL_ROWS != 0)
        valueLength = (int) (numberOfRows % ColumnShardBuilder.PROPOSAL_ROWS);

      long[] pageValue = new long[valueLength];
      long nextPageValueId = 0;

      for (int i = 0; i < valueLength; i++) {
        if (nextRowIdToValue == null && navigableRowIdToValuesIterator.hasNext())
          nextRowIdToValue = navigableRowIdToValuesIterator.next();

        long rowId = ((long) pageNo) * ColumnShardBuilder.PROPOSAL_ROWS + i;

        long valueId;
        if (nextRowIdToValue != null && nextRowIdToValue.getKey() == rowId) {
          valueId = nextRowIdToValue.getValue();
          nextRowIdToValue = null;
        } else {
          valueId = defaultValueId;
        }

        // Adjust ID that was stored in columnDict, if it has been adjusted when building the column dictionary above.
        if (idChangeMap != null && idChangeMap.containsKey(valueId))
          valueId = idChangeMap.get(valueId);

        // give this value a new ID which is valid for this column page
        if (!valueToId.containsKey(valueId)) {
          valueToId.put(valueId, nextPageValueId++);
        }

        // remember the new ID as value
        pageValue[i] = valueToId.get(valueId);
      }

      ColumnPageBuilder pageBuilder = new ColumnPageBuilder(columnPageFactory);
      // TODO add firstRowInShard
      long firstRowId = ((long) pageNo) * ColumnShardBuilder.PROPOSAL_ROWS;
      pageBuilder.withFirstRowId(firstRowId).withValueMap(valueToId).withValues(pageValue)
          .withColumnPageName(name + "#" + firstRowId);

      ColumnPage newPage = pageBuilder.build();
      pages.put(firstRowId, newPage);
    }

    return res;
  }

}
