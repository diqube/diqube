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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnPageFactory;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.loader.LoaderColumnInfo;

/**
 * Manages all {@link ColumnShardBuilder}s for loading one {@link TableShard}.
 * 
 * <p>
 * This class is a convenience class for the loaders to use, so the loaders do not need to take care of generics
 * themselves.
 *
 * @author Bastian Gloeckle
 */
public class ColumnShardBuilderManager {
  private volatile Map<String, ColumnShardBuilder<String>> stringBuilders = new ConcurrentHashMap<>();
  private volatile Map<String, ColumnShardBuilder<Long>> longBuilders = new ConcurrentHashMap<>();
  private volatile Map<String, ColumnShardBuilder<Double>> doubleBuilders = new ConcurrentHashMap<>();

  private ColumnShardFactory columnShardFactory;
  private ColumnPageFactory columnPageFactory;
  private LoaderColumnInfo columnInfo;
  private long firstRowIdInShard;

  public ColumnShardBuilderManager(ColumnShardFactory columnShardFactory, ColumnPageFactory columnPageFactory,
      LoaderColumnInfo columnInfo, long firstRowIdInShard) {
    this.columnShardFactory = columnShardFactory;
    this.columnPageFactory = columnPageFactory;
    this.columnInfo = columnInfo;
    this.firstRowIdInShard = firstRowIdInShard;
  }

  /**
   * @return Column Names of all columns that received some data with {@link #addValues(String, Object[], long)}.
   */
  public Set<String> getAllColumnsWithValues() {
    Set<String> res = new HashSet<>(stringBuilders.keySet());
    res.addAll(longBuilders.keySet());
    res.addAll(doubleBuilders.keySet());
    return res;
  }

  /**
   * Calls {@link ColumnShardBuilder#addValues(Object[], Long)} for the column accordingly.
   * 
   * @param colName
   * @param values
   * @param firstRowId
   */
  public void addValues(String colName, Object[] values, long firstRowId) {
    switch (columnInfo.getFinalColumnType(colName)) {
    case STRING:
      if (!stringBuilders.containsKey(colName)) {
        synchronized (stringBuilders) {
          if (!stringBuilders.containsKey(colName)) {
            stringBuilders.put(colName,
                new ColumnShardBuilder<String>(columnShardFactory, columnPageFactory, colName, firstRowIdInShard));
          }
        }
      }
      stringBuilders.get(colName).addValues((String[]) values, firstRowId);
      break;
    case LONG:
      if (!longBuilders.containsKey(colName)) {
        synchronized (longBuilders) {
          if (!longBuilders.containsKey(colName)) {
            longBuilders.put(colName,
                new ColumnShardBuilder<Long>(columnShardFactory, columnPageFactory, colName, firstRowIdInShard));
          }
        }
      }
      longBuilders.get(colName).addValues((Long[]) values, firstRowId);
      break;
    case DOUBLE:
      if (!doubleBuilders.containsKey(colName)) {
        synchronized (doubleBuilders) {
          if (!doubleBuilders.containsKey(colName)) {
            doubleBuilders.put(colName,
                new ColumnShardBuilder<Double>(columnShardFactory, columnPageFactory, colName, firstRowIdInShard));
          }
        }
      }
      doubleBuilders.get(colName).addValues((Double[]) values, firstRowId);
      break;
    }
  }

  /**
   * Executes {@link ColumnShardBuilder#build()}.
   */
  public StandardColumnShard build(String colName) {
    switch (columnInfo.getFinalColumnType(colName)) {
    case STRING:
      return stringBuilders.get(colName).build();
    case LONG:
      return longBuilders.get(colName).build();
    case DOUBLE:
      return doubleBuilders.get(colName).build();
    }
    return null;
  }
}
