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
package org.diqube.execution.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.diqube.data.Table;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.util.DiqubeIterables;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Default implementation of a cache that caches data in the context of a {@link Table}.
 *
 * @author Bastian Gloeckle
 */
public class DefaultTableCache implements WritableTableCache {
  private static final Logger logger = LoggerFactory.getLogger(DefaultTableCache.class);

  private ConcurrentMap<Long, ConcurrentMap<String, ColumnShard>> allCachesByTableShard = new ConcurrentHashMap<>();

  private ConcurrentMap<ColId, AtomicLong> counts = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<ColIdCount> topCounts = new ConcurrentSkipListSet<>();
  private ConcurrentMap<ColId, Long> memoryOfCols = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<ColId> currentlyCachedCols = new ConcurrentSkipListSet<>();

  private Object updateCacheSync = new Object();

  /** gets write-locked when cleanup is running. Use read-lock to execute something while not cleaning up. */
  private ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock();
  private Random cleanupRandom = new Random();

  private long maxMemoryBytes;

  /* package */ DefaultTableCache(long maxMemoryBytes) {
    this.maxMemoryBytes = maxMemoryBytes;
  }

  @Override
  public ColumnShard getCachedColumnShard(long firstRowIdTableShard, String colName) {
    ConcurrentMap<String, ColumnShard> cache = allCachesByTableShard.get(firstRowIdTableShard);
    if (cache == null)
      return null;

    return cache.get(colName);
  }

  @Override
  public Collection<ColumnShard> getAllCachedColumnShards(long firstRowIdInTableShard) {
    ConcurrentMap<String, ColumnShard> cache = allCachesByTableShard.get(firstRowIdInTableShard);
    if (cache == null)
      return new ArrayList<>();
    return new ArrayList<>(cache.values());
  }

  @Override
  public void registerUsageOfColumnShardPossiblyCache(long firstRowIdInTableShard, ColumnShard createdColumnShard) {
    cleanupLock.readLock().lock(); // do not execute cleanup while we're inside this block!
    try {
      ColId colId = new ColId(firstRowIdInTableShard, createdColumnShard.getName());

      AtomicLong count = counts.computeIfAbsent(colId, id -> new AtomicLong(Long.MIN_VALUE));
      long oldCount = count.getAndIncrement();
      long newCount = oldCount + 1;

      memoryOfCols.computeIfAbsent(colId, ci -> createdColumnShard.calculateApproximateSizeInBytes());

      // we add our col with its count to "topCounts". Note: If this col now is used often enough to get into the cache,
      // actually another thread calling this method might expect our column to be added to the cache already (and
      // remove other cols for its decision accordingly). We do not really care about this, though -> in that case the
      // cache might be smaller than needed for a short time.
      ColIdCount newColIdCount = new ColIdCount(colId, newCount);
      topCounts.add(newColIdCount);
      topCounts.remove(new ColIdCount(colId, oldCount));

      // we retry to identify if we have to add the new col/what other cols to remove to/from the cache. This is because
      // while inspecting the situation we might have multiple threads doing the same simultaneously, even with the same
      // ColId! There we find a decision what we'd like to do, then enter a sync-block and validate if the data we based
      // our decision on is still valid and only if it is, we execute our decision.
      boolean retry = true;
      while (retry) {
        // we collect the colIds that we inspected in "counts" in the right order.
        List<ColIdCount> curInspectedCountColIds = new ArrayList<>();

        Set<ColId> colIdsVisited = new HashSet<>();
        Set<ColId> colIdsThatShouldBeCached = new HashSet<>();
        long memory = 0L;
        for (ColIdCount colIdCount : topCounts) {
          curInspectedCountColIds.add(colIdCount);

          if (colIdsVisited.contains(colIdCount.getLeft()))
            // ColIds might be available multiple times in "count" (they are first added, then removed, see above).
            // Therefore make sure we just take the maximum "count" into account of a ColId.
            continue;
          colIdsVisited.add(colIdCount.getLeft());

          long nextMemory = memoryOfCols.get(colIdCount.getLeft());
          if (memory + nextMemory > maxMemoryBytes)
            break;
          colIdsThatShouldBeCached.add(colIdCount.getLeft());
        }

        // decide what we have to do
        Set<ColId> curCurrentlyCachedCols = new HashSet<>(currentlyCachedCols);
        Collection<ColId> colIdsToBeRemovedFromCache =
            new ArrayList<>(Sets.difference(curCurrentlyCachedCols, colIdsThatShouldBeCached));

        boolean shouldAddNewColToCache = colIdsThatShouldBeCached.contains(colId);

        if (!colIdsToBeRemovedFromCache.isEmpty() || colIdsThatShouldBeCached.contains(colId)) {
          synchronized (updateCacheSync) {
            if (!curCurrentlyCachedCols.equals(currentlyCachedCols)
                || !DiqubeIterables.startsWith(topCounts, curInspectedCountColIds))
              // retry as the data structures we based our decisions on have changed.
              continue;

            colIdsToBeRemovedFromCache.forEach(id -> removeFromCache(id));

            if (shouldAddNewColToCache)
              addToCache(colId, createdColumnShard);

            // we succeeded!
            retry = false;
          }
        }
      }
    } finally {
      cleanupLock.readLock().unlock();
    }

    if (cleanupRandom.nextInt(128) < 4) // use 128 as base so random is implemented faster. Scale "3" (%) equally (1.28)
      // execute approx on 3% of calls.
      intermediaryCleanup();
  }

  /**
   * Executes a cleanup of data structures of this instance - execute once and then to not pollute memory!
   */
  private void intermediaryCleanup() {
    cleanupLock.writeLock().lock();
    try {
      logger.trace("Executing cache cleanup...");

      // remove memory information of all cols that are not cached - Next time someone wants to insert such a col, we
      // can recalculate the size without problems.
      memoryOfCols.keySet().retainAll(currentlyCachedCols);

      // clean topCounts: Remove any ColIdCounts of ColIds where we have bigger counts (=earlier in topCounts) and/or
      // which are not currently cached.
      Set<ColId> colIdsVisited = new HashSet<>();
      for (Iterator<ColIdCount> it = topCounts.iterator(); it.hasNext();) {
        ColIdCount curCnt = it.next();

        if (colIdsVisited.contains(curCnt.getLeft())) {
          it.remove();
          continue;
        }
        colIdsVisited.add(curCnt.getLeft());

        if (!currentlyCachedCols.contains(curCnt.getLeft()))
          it.remove();
      }
    } finally {
      cleanupLock.writeLock().unlock();
    }
  }

  /**
   * Removes data of a sprecific colId from the cache. Call only when synced on {@link #updateCacheSync} and inside a
   * lock of {@link #cleanupLock}!
   */
  private void removeFromCache(ColId colId) {
    logger.trace("Removing column from cache: {}", colId);
    currentlyCachedCols.remove(colId);
    allCachesByTableShard.get(colId.getLeft()).remove(colId.getRight());
    if (allCachesByTableShard.get(colId.getLeft()).isEmpty())
      allCachesByTableShard.remove(colId.getLeft());
  }

  /**
   * Add data of a sprecific colId to the cache. Call only when synced on {@link #updateCacheSync} and inside a lock of
   * {@link #cleanupLock}!
   */
  private void addToCache(ColId colId, ColumnShard colShard) {
    logger.trace("Adding column to cache: {}", colId);
    currentlyCachedCols.add(colId);
    ConcurrentMap<String, ColumnShard> tableShardCache =
        allCachesByTableShard.computeIfAbsent(colId.getLeft(), id -> new ConcurrentHashMap<>());
    tableShardCache.put(colId.getRight(), colShard);
  }

  /**
   * Identifies a column shard inside a table shard.
   * 
   * <p>
   * Left: {@link TableShard#getLowestRowId()} <br/>
   * Right: Column name
   */
  private static class ColId extends Pair<Long, String> {
    public ColId(Long left, String right) {
      super(left, right);
    }
  }

  /**
   * A {@link ColId} combined with the number of times it was used.
   * 
   * <p>
   * This is {@link Comparable} and compares first by "count" (descending) and then by {@link ColId}.
   */
  private static class ColIdCount extends Pair<ColId, Long> {
    public ColIdCount(ColId left, Long right) {
      super(left, right);
    }

    @Override
    public int compareTo(Pair<ColId, Long> o) {
      int countCmp = getRight().compareTo(o.getRight());
      if (countCmp != 0)
        return -1 * countCmp;
      return getLeft().compareTo(o.getLeft());
    }
  }
}
