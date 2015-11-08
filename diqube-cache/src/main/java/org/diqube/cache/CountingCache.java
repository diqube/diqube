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
package org.diqube.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.diqube.util.DiqubeIterables;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Default implementation of a cache that counts usage of elements and caches the most used objects, but adhering to a
 * memory cap.
 * 
 * <p>
 * Assumes that values identified by a key pair (K1,K2) do not change, not even when re-inserted into the cache.
 * 
 * <p>
 * As this implementation counts the offers, these counts are not internally cleared ever. This means that the memory
 * used by this cache will increase statically over time if the keys keep changing. <b>Take therefore special care of
 * what to use as keys! Do NOT use any randomly generated IDs etc (e.g. {@link UUID#randomUUID()})!</b>
 * 
 * <p>
 * It caches those values that were used the most often times - it therefore counts the usages of these on calls to
 * {@link #offer(Object, Object, Object)}. This cache caches up to a maximum memory size, which is calculated using a
 * {@link MemoryConsumptionProvider}. If there are multiple values used the same amount of times and they are at the
 * that max-memory border, the ColumnShards are ordered by their key2 (of type K2) - the columns with the "lesser" key2s
 * may be cached.
 * 
 * <p>
 * This cache maintains itself and does execute cleanup actions on internally used data structures at its own
 * discretion.
 *
 * @author Bastian Gloeckle
 */
public class CountingCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V>
    implements WritableCache<K1, K2, V> {
  private static final Logger logger = LoggerFactory.getLogger(CountingCache.class);

  /**
   * The default internal cleanup strategy cleans up randomly in approx. 3% of cases.
   * 
   * Use 128 as base so random is implemented faster. Scale "3" (%) equally (* 1.28).
   */
  private static final InternalCleanupStrategy DEFAULT_CLEANUP_STRATEGY =
      () -> ThreadLocalRandom.current().nextInt(128) < 4;

  private ConcurrentMap<K1, ConcurrentMap<K2, V>> caches = new ConcurrentHashMap<>();

  private ConcurrentMap<CacheId, AtomicLong> counts = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<CacheIdCount> topCounts = new ConcurrentSkipListSet<>();
  private ConcurrentMap<CacheId, Long> memoryConsumption = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<CacheId> currentlyCachedCacheIds = new ConcurrentSkipListSet<>();

  private Object updateCacheSync = new Object();

  /** gets write-locked when cleanup is running. Use read-lock to execute something while not cleaning up. */
  private ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock();
  private InternalCleanupStrategy cleanupStrategy;

  private long maxMemoryBytes;

  private MemoryConsumptionProvider<V> memoryConsumptionProvider;

  public CountingCache(long maxMemoryBytes, MemoryConsumptionProvider<V> memoryConsumptionProvider) {
    this(maxMemoryBytes, DEFAULT_CLEANUP_STRATEGY, memoryConsumptionProvider);
  }

  /**
   * Constructor mainly for tests to customize the cleanup strategy.
   */
  public CountingCache(long maxMemoryBytes, InternalCleanupStrategy cleanupStrategy,
      MemoryConsumptionProvider<V> memoryConsumptionProvider) {
    this.maxMemoryBytes = maxMemoryBytes;
    this.cleanupStrategy = cleanupStrategy;
    this.memoryConsumptionProvider = memoryConsumptionProvider;
  }

  @Override
  public V get(K1 key1, K2 key2) {
    ConcurrentMap<K2, V> cache = caches.get(key1);
    if (cache == null)
      return null;

    return cache.get(key2);
  }

  @Override
  public Collection<V> getAll(K1 key1) {
    ConcurrentMap<K2, V> cache2 = caches.get(key1);
    if (cache2 == null)
      return new ArrayList<>();
    return new ArrayList<>(cache2.values());
  }

  @Override
  public boolean offer(K1 key1, K2 key2, V value) {
    boolean addedToCache = false;

    cleanupLock.readLock().lock(); // do not execute cleanup while we're inside this block!
    try {
      CacheId cacheId = new CacheId(key1, key2);

      AtomicLong count = counts.computeIfAbsent(cacheId, id -> new AtomicLong(Long.MIN_VALUE));
      long oldCount = count.getAndIncrement();
      long newCount = oldCount + 1;

      memoryConsumption.computeIfAbsent(cacheId, ci -> memoryConsumptionProvider.getMemoryConsumption(value));

      // we add our value with its count to "topCounts". Note: If this value now is used often enough to get into the
      // cache,
      // actually another thread calling this method might expect our value to be added to the cache already (and
      // remove other values for its decision accordingly). We do not really care about this, though -> in that case the
      // cache might be smaller than needed for a short time.
      CacheIdCount newColIdCount = new CacheIdCount(cacheId, newCount);
      topCounts.add(newColIdCount);
      topCounts.remove(new CacheIdCount(cacheId, oldCount));

      // we retry to identify if we have to add the new value/what other values to remove to/from the cache. This is
      // because
      // while inspecting the situation we might have multiple threads doing the same simultaneously, even with the same
      // CacheId! There we find a decision what we'd like to do, then enter a sync-block and validate if the data we
      // based
      // our decision on is still valid and only if it is, we execute our decision.
      boolean retry = true;
      while (retry) {
        // we collect the colIds that we inspected in "counts" in the right order.
        List<CacheIdCount> curInspectedCountCacheIds = new ArrayList<>();

        Set<CacheId> cacheIdsVisited = new HashSet<>();
        Set<CacheId> cacheIdsThatShouldBeCached = new HashSet<>();
        long memory = 0L;
        for (CacheIdCount cacheIdCount : topCounts) {
          curInspectedCountCacheIds.add(cacheIdCount);

          if (cacheIdsVisited.contains(cacheIdCount.getLeft()))
            // CacheIds might be available multiple times in "count" (they are first added, then removed, see above).
            // Therefore make sure we just take the maximum "count" into account of a CacheId.
            continue;
          cacheIdsVisited.add(cacheIdCount.getLeft());

          // memory consumption is definitely available, since it cannot have been cleaned up, as the internal cleanup
          // cannot run simultaneously!
          long nextMemory = memoryConsumption.get(cacheIdCount.getLeft());
          if (memory + nextMemory > maxMemoryBytes)
            break;
          memory += nextMemory;
          cacheIdsThatShouldBeCached.add(cacheIdCount.getLeft());
        }

        // decide what we have to do
        Set<CacheId> curCurrentlyCachedCacheIds = new HashSet<>(currentlyCachedCacheIds);
        Collection<CacheId> cacheIdsToBeRemovedFromCache =
            new ArrayList<>(Sets.difference(curCurrentlyCachedCacheIds, cacheIdsThatShouldBeCached));

        boolean shouldAddNewCacheIdToCache =
            !curCurrentlyCachedCacheIds.contains(cacheId) && cacheIdsThatShouldBeCached.contains(cacheId);

        if (!cacheIdsToBeRemovedFromCache.isEmpty() || cacheIdsThatShouldBeCached.contains(cacheId)) {
          synchronized (updateCacheSync) {
            if (!curCurrentlyCachedCacheIds.equals(currentlyCachedCacheIds)
                || !DiqubeIterables.startsWith(topCounts, curInspectedCountCacheIds))
              // retry as the data structures we based our decisions on have changed.
              continue;

            cacheIdsToBeRemovedFromCache.forEach(id -> removeFromCache(id));

            if (shouldAddNewCacheIdToCache) {
              addToCache(cacheId, value);
              addedToCache = true;
            }

            // we succeeded!
            retry = false;
          }
        } else
          // we do not need to take any action, so we're done!
          retry = false;
      }
    } finally {
      cleanupLock.readLock().unlock();
    }

    if (cleanupStrategy.executeCleanup())
      intermediaryCleanup();

    return addedToCache;
  }

  /**
   * Executes a cleanup of data structures of this instance - execute once and then to not pollute memory!
   */
  private void intermediaryCleanup() {
    cleanupLock.writeLock().lock();
    try {
      logger.trace("Executing cache cleanup...");

      // clean topCounts: Remove any CacheIdCounts of CacheIds where we have bigger counts (=earlier in topCounts)
      // and/or which are not currently cached.
      boolean lastTopCountWasCached = true;
      Set<CacheId> cacheIdsVisited = new HashSet<>();
      CacheId notCachedInterestingCacheId = null;
      for (Iterator<CacheIdCount> it = topCounts.iterator(); it.hasNext();) {
        CacheIdCount curCnt = it.next();

        if (cacheIdsVisited.contains(curCnt.getLeft())) {
          it.remove();
          continue;
        }
        cacheIdsVisited.add(curCnt.getLeft());

        boolean curCntIsCached = currentlyCachedCacheIds.contains(curCnt.getLeft());

        if (!curCntIsCached && !lastTopCountWasCached)
          // remove all entries from topCount but leave only the cached ones and the one right after that. The latter
          // one is needed to make sure not new entries are cached (= are in topCount and memory does match), but would
          // actually have a lower CacheIdCount value than the one following the cachedTopCounts (this can happen e.g.
          // if the one after the cached topCounts is a very memory-intensive value, does therefore not get cached, but
          // the next value with a lower count would have a memory consumption that would fit in the cache). We do
          // not cache any CacheIdCounts that are lower than the first one that does not fit into memory any more.
          it.remove();
        else if (!curCntIsCached && lastTopCountWasCached)
          notCachedInterestingCacheId = curCnt.getLeft();

        lastTopCountWasCached = curCntIsCached;
      }

      // remove memory information of all cacheIds that are not interesting (=cached values are interesting, if there
      // is
      // another value left in topCount, that is interesting, too) - Next time someone wants to insert a value we remove
      // now, we can recalculate the size without problems.
      Collection<CacheId> retainCacheIds = currentlyCachedCacheIds;
      if (notCachedInterestingCacheId != null)
        retainCacheIds =
            Sets.newHashSet(Iterables.concat(currentlyCachedCacheIds, Arrays.asList(notCachedInterestingCacheId)));
      memoryConsumption.keySet().retainAll(retainCacheIds);

      // Note: do not clean up "counts" field, as we obviously need to keep track of all counts and should not evict
      // that ever.
    } finally {
      cleanupLock.writeLock().unlock();
    }
  }

  /**
   * Removes data of a specific {@link CacheId} from the cache. Call only when synced on {@link #updateCacheSync} and
   * inside a lock of {@link #cleanupLock}!
   */
  private void removeFromCache(CacheId colId) {
    logger.trace("Removing from cache: {}", colId);
    currentlyCachedCacheIds.remove(colId);
    caches.get(colId.getLeft()).remove(colId.getRight());
    if (caches.get(colId.getLeft()).isEmpty())
      caches.remove(colId.getLeft());
  }

  /**
   * Add data of a specific {@link CacheId} to the cache. Call only when synced on {@link #updateCacheSync} and inside a
   * lock of {@link #cleanupLock}!
   */
  private void addToCache(CacheId cacheId, V value) {
    logger.trace("Adding to cache: {}", cacheId);
    currentlyCachedCacheIds.add(cacheId);
    ConcurrentMap<K2, V> k1Cache = caches.computeIfAbsent(cacheId.getLeft(), id -> new ConcurrentHashMap<>());
    k1Cache.put(cacheId.getRight(), value);
  }

  @Override
  public int size() {
    return currentlyCachedCacheIds.size();
  }

  /** for testing only! */
  protected long getMaxMemoryBytes() {
    return maxMemoryBytes;
  }

  /** for testing only! */
  protected void removeFromCache(K1 key1, K2 key2) {
    cleanupLock.writeLock().lock();
    try {
      synchronized (updateCacheSync) {
        removeFromCache(new CacheId(key1, key2));
      }
    } finally {
      cleanupLock.writeLock().unlock();
    }
  }

  /**
   * Identifies a (cached) entity within the cache.
   * 
   * <p>
   * Left: key part 1 <br/>
   * Right: key part 2
   */
  private class CacheId extends Pair<K1, K2> {
    public CacheId(K1 left, K2 right) {
      super(left, right);
    }
  }

  /**
   * A {@link CacheId} combined with the number of times that element was offered.
   * 
   * <p>
   * This is {@link Comparable} and compares first by "count" (descending) and then by {@link CacheId}.
   */
  private class CacheIdCount extends Pair<CacheId, Long> {
    public CacheIdCount(CacheId left, Long right) {
      super(left, right);
    }

    @Override
    public int compareTo(Pair<CacheId, Long> o) {
      int countCmp = getRight().compareTo(o.getRight());
      if (countCmp != 0)
        return -1 * countCmp;
      return getLeft().compareTo(o.getLeft());
    }
  }

  /**
   * Mainly for testing: Extract strategy on when to do an internal cleanup.
   */
  /* package */ static interface InternalCleanupStrategy {
    /**
     * @return true if cleanup should be execued, false otherwise.
     */
    public boolean executeCleanup();
  }

  public static interface MemoryConsumptionProvider<V> {
    public long getMemoryConsumption(V value);
  }
}
