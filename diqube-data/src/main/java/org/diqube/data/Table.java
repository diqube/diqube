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
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.diqube.data.colshard.ColumnShard;

/**
 * A {@link Table} is the basic container of any data.
 * 
 * <p>
 * From a logical point of view, a Table is made up of columns and rows, whereas each row can hold an arbitrary complex
 * object whose values are split into separate columns ({@link ColumnShard}) on import time.
 * 
 * <p>
 * Technically, a table consists of {@link TableShard}s of which one or multiple might be available on the current
 * machine, and others being resident on other cluster machines. Each {@link TableShard} contains the data of a subset
 * of the tables rows. Each TableShard then contains a set of {@link ColumnShard}s, which in turn contain the actual
 * data. Note that each TableShard might contain a different set of columns, as each TableShard only materializes those
 * Columns, that it actually has data for.
 *
 * @author Bastian Gloeckle
 */
public class Table {
  private String name;
  private Collection<TableShard> shards;
  private ReentrantReadWriteLock shardsLock = new ReentrantReadWriteLock();

  /* package */ Table(String name, Collection<TableShard> shards) {
    this.name = name;
    this.shards = shards;
  }

  /**
   * @return Name of this table
   */
  public String getName() {
    return name;
  }

  /**
   * @return Shards of this table, unmodifiable.
   */
  public Collection<TableShard> getShards() {
    shardsLock.readLock().lock();
    try {
      return Collections.unmodifiableCollection(shards);
    } finally {
      shardsLock.readLock().unlock();
    }
  }

  /**
   * Adds a {@link TableShard} to this table.
   * 
   * @throws TableShardsOverlappingException
   *           If the rows served by the new tableShard overlap with a tableShard already in the table. If the exception
   *           is thrown, the new TableShard is not added to the table.
   */
  public void addTableShard(TableShard tableShard) throws TableShardsOverlappingException {
    shardsLock.writeLock().lock();
    try {
      // exchange list to make sure that lists already returned by getShards() will be iterable.
      List<TableShard> newShards = new ArrayList<>(shards);
      newShards.add(tableShard);

      NavigableMap<Long, Long> rowIdMap = new TreeMap<>();
      for (TableShard shard : newShards) {
        if (rowIdMap.containsKey(shard.getLowestRowId()))
          throw new TableShardsOverlappingException("Two TableShards with first row ID " + shard.getLowestRowId());
        rowIdMap.put(shard.getLowestRowId(), shard.getLowestRowId() + shard.getNumberOfRowsInShard() - 1);
      }
      for (Long lowestRowId : rowIdMap.keySet()) {
        Long nextKey = rowIdMap.ceilingKey(lowestRowId + 1);
        if (nextKey != null && nextKey <= rowIdMap.get(lowestRowId))
          throw new TableShardsOverlappingException(
              "There are overlapping TableShards. One contains data for rowIds " + lowestRowId + "-"
                  + rowIdMap.get(lowestRowId) + " and another for rowIds " + nextKey + "-" + rowIdMap.get(nextKey));
      }

      shards = newShards;
    } finally {
      shardsLock.writeLock().unlock();
    }
  }

  /**
   * Remove the given tableShard from this table.
   * 
   * @return true if the tableShard was contained in this table.
   */
  public boolean removeTableShard(TableShard tableShard) {
    shardsLock.writeLock().lock();
    try {
      // exchange list to make sure that lists already returned by getShards() will be iterable.
      List<TableShard> newShards = new ArrayList<>(shards);
      if (!newShards.remove(tableShard))
        return false;
      shards = newShards;
      return true;
    } finally {
      shardsLock.writeLock().unlock();
    }
  }

  public class TableShardsOverlappingException extends Exception {
    private static final long serialVersionUID = 1L;

    public TableShardsOverlappingException(String msg) {
      super(msg);
    }
  }
}
