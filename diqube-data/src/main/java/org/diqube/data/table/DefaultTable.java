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
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default implementation for a {@link Table} which contains raw data that was loaded from input files.
 *
 * @author Bastian Gloeckle
 */
public class DefaultTable implements AdjustableTable {
  private String name;
  private Collection<TableShard> shards;
  private ReentrantReadWriteLock shardsLock = new ReentrantReadWriteLock();

  /* package */ DefaultTable(String name, Collection<TableShard> shards) {
    this.name = name;
    this.shards = shards;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<TableShard> getShards() {
    shardsLock.readLock().lock();
    try {
      return Collections.unmodifiableCollection(new ArrayList<>(shards));
    } finally {
      shardsLock.readLock().unlock();
    }
  }

  @Override
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

  @Override
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

}
