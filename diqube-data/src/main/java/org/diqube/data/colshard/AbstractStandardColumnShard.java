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
package org.diqube.data.colshard;

import java.util.NavigableMap;

import org.diqube.data.ColumnType;
import org.diqube.data.Dictionary;

/**
 * Abstract implementation of a {@link StandardColumnShard}.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractStandardColumnShard implements StandardColumnShard {
  protected Dictionary<?> columnShardDictionary;

  protected String name;

  protected ColumnType columnType;

  /**
   * Map from firstId to {@link ColumnPage}
   */
  protected NavigableMap<Long, ColumnPage> pages;

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
      Dictionary<?> columnShardDictionary) {
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

}
