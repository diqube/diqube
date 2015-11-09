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
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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
 * what to use as keys! Do NOT use any randomly generated IDs etc (e.g. {@link UUID#randomUUID()}) without specifying a
 * meaningful {@link CountCleanupStrategy}!</b> By default counts will never be cleaned up.
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
 * <p>
 * This cache is a {@link FlaggingCache}, which means that is capable of prohibiting specific elements from being
 * evicted from the cache for a certain amount of time. This cache implements this behavior without an additional
 * thread. Therefore it might take a while to actually evict values that have been flagged: This will happen on calls to
 * {@link #offer(Comparable, Comparable, Object)} and {@link #offerAndFlag(Comparable, Comparable, Object, long)};
 * additionally one can trigger it using {@link #consolidate()}.
 * 
 * <p>
 * The flagged elements memory size does <b>NOT</b> count towards the memory cap! This means that this cache might
 * actually take up "maxMemory + x" memory, where x is the sum of sizes of all flagged entries. On the other hand, if
 * there is free memory (up to the cap), but there are not-longer-flagged entries, these are not evicted from the cache
 * until the cap is reached (this is obviously true, because since we're under the cap, all entries have an entry in
 * {@link #topCounts} and are therefore regular entries in the cache, although that flagged entry was just additionally
 * flagged).
 *
 * @author Bastian Gloeckle
 */
public class CountingCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V>
    implements WritableFlaggingCache<K1, K2, V> {
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

  /**
   * CacheIds which are flagged currently to the current information about the flag.
   * 
   * Items are removed from here by {@link #removeOldFlaggedCacheIds()}. Note to sync on the {@link FlagInfo} object if
   * changing anything of interest for {@link #removeOldFlaggedCacheIds()} in the {@link FlagInfo}.
   */
  private ConcurrentMap<CacheId, FlagInfo> flaggedChacheIds = new ConcurrentHashMap<>();
  /**
   * Timeout time in nanos to the CacheId that was flagged.
   */
  private ConcurrentSkipListMap<Long, CacheId> flaggedTimes = new ConcurrentSkipListMap<>();

  private Object updateCacheSync = new Object();

  /** gets write-locked when cleanup is running. Use read-lock to execute something while not cleaning up. */
  private ReentrantReadWriteLock cleanupLock = new ReentrantReadWriteLock();
  private InternalCleanupStrategy cleanupStrategy;

  private long maxMemoryBytes;

  private MemoryConsumptionProvider<V> memoryConsumptionProvider;

  private CountCleanupStrategy<K1, K2> countCleanupStrategy;

  public CountingCache(long maxMemoryBytes, MemoryConsumptionProvider<V> memoryConsumptionProvider) {
    this(maxMemoryBytes, DEFAULT_CLEANUP_STRATEGY, memoryConsumptionProvider);
  }

  public CountingCache(long maxMemoryBytes, MemoryConsumptionProvider<V> memoryConsumptionProvider,
      CountCleanupStrategy<K1, K2> countCleanupStrategy) {
    this(maxMemoryBytes, DEFAULT_CLEANUP_STRATEGY, memoryConsumptionProvider, countCleanupStrategy);
  }

  public CountingCache(long maxMemoryBytes, InternalCleanupStrategy cleanupStrategy,
      MemoryConsumptionProvider<V> memoryConsumptionProvider) {
    this(maxMemoryBytes, cleanupStrategy, memoryConsumptionProvider,
        /* never to count cleanups! */
        (countsUpForCleanup, allCounts) -> null);
  }

  public CountingCache(long maxMemoryBytes, InternalCleanupStrategy cleanupStrategy,
      MemoryConsumptionProvider<V> memoryConsumptionProvider, CountCleanupStrategy<K1, K2> countCleanupStrategy) {
    this.maxMemoryBytes = maxMemoryBytes;
    this.cleanupStrategy = cleanupStrategy;
    this.memoryConsumptionProvider = memoryConsumptionProvider;
    this.countCleanupStrategy = countCleanupStrategy;
  }

  @Override
  public V get(K1 key1, K2 key2) {
    ConcurrentMap<K2, V> cache = caches.get(key1);
    if (cache == null)
      return null;

    return cache.get(key2);
  }

  @Override
  public V flagAndGet(K1 key1, K2 key2, long flagUntilNanos) {
    CacheId cacheId = new CacheId(key1, key2);
    while (true) {
      V res = get(key1, key2);
      if (res == null)
        return null;

      flag(cacheId, flagUntilNanos);

      // re-check that the element we got is still in cache.
      if (res == get(key1, key2))
        return res;

      // Here: If an offer of the same CacheId than this method happens exactly here, we might keep something in the
      // cache although we would remove the flaggedCacheId right away again. But that is not as bad, since in the next
      // call to #consolidate this mistake will be corrected.

      // element changed, remove flag and retry.
      flaggedChacheIds.compute(cacheId, (k, v) -> {
        // no need to sync here: If the following if is "true", we added that cacheId to flaggedCacheIds just a few
        // lines above, therefore we're of no interest to #removeOldFlaggedCacheIds yet. If the "if" is "false", we do
        // not care anyway, since then this method does not change anything of interest to #removeOldFlaggedCacheIds.
        if (v.getFlagCount().decrementAndGet() == 0)
          return null;
        return v;
      });
    }
  }

  /**
   * Add a given cacheId to {@link #flaggedTimes} and {@link #flaggedChacheIds}.
   * 
   * Will adjust the given flagUntilNanos so we do not get collisions in {@link #flaggedTimes}.
   * 
   * @return The flagUntilNanos value actually used.
   */
  private long flag(CacheId cacheId, long flagUntilNanos) {
    while (flaggedTimes.putIfAbsent(flagUntilNanos, cacheId) != null)
      flagUntilNanos++;

    flaggedChacheIds.merge(cacheId, new FlagInfo(flagUntilNanos), (oldValue, newValue) -> {
      synchronized (oldValue) { // sync to stay valid according to #removeOldFlaggedCacheIds
        newValue.getFlagCount().addAndGet(oldValue.getFlagCount().get());
        newValue.getNewestTimeoutNanos().getAndAccumulate(oldValue.getNewestTimeoutNanos().get(), Long::max);
        return newValue;
      }
    });
    return flagUntilNanos;
  }

  @Override
  public Collection<V> getAll(K1 key1) {
    ConcurrentMap<K2, V> cache2 = caches.get(key1);
    if (cache2 == null)
      return new ArrayList<>();
    return new ArrayList<>(cache2.values());
  }

  @Override
  public V offerAndFlag(K1 key1, K2 key2, V value, long flagUntilNanos) {
    return offerAndFlag(key1, key2, value, flagUntilNanos, 1);
  }

  /**
   * Just like {@link #offerAndFlag(Comparable, Comparable, Object, long)}, but specify the number of times this element
   * has been "used".
   * 
   * @param countDelta
   *          Number of times the offered element has been "used".
   */
  public V offerAndFlag(K1 key1, K2 key2, V value, long flagUntilNanos, long countDelta) {
    flag(new CacheId(key1, key2), flagUntilNanos);
    offer(key1, key2, value, countDelta);
    return get(key1, key2);
  }

  @Override
  public boolean offer(K1 key1, K2 key2, V value) {
    return offer(key1, key2, value, 1);
  }

  /**
   * Just like {@link #offer(Comparable, Comparable, Object)}, but specify the number of times this element has been
   * "used".
   * 
   * @param countDelta
   *          Number of times the offered element has been "used".
   */
  public boolean offer(K1 key1, K2 key2, V value, long countDelta) {
    // Implementation details: We adjust internal data structures here, but leave #cache unchanged. Based on this, this
    // method then calls #consolidate to consolidate the caches, too.

    removeOldFlaggedCacheIds();

    boolean addedToCache = false;

    cleanupLock.readLock().lock(); // do not execute cleanup while we're inside this block!
    try {
      CacheId cacheId = new CacheId(key1, key2);

      AtomicLong count = counts.computeIfAbsent(cacheId, id -> new AtomicLong(0L));
      long oldCount = count.getAndAdd(countDelta);
      long newCount = oldCount + countDelta;

      memoryConsumption.computeIfAbsent(cacheId, ci -> memoryConsumptionProvider.getMemoryConsumptionBytes(value));

      // we add our value with its count to "topCounts". Note: If this value now is used often enough to get into the
      // cache,
      // actually another thread calling this method might expect our value to be added to the cache already (and
      // remove other values for its decision accordingly). We do not really care about this, though -> in that case the
      // cache might be smaller than needed for a short time.
      CacheIdCount newColIdCount = new CacheIdCount(cacheId, newCount);
      topCounts.add(newColIdCount);
      topCounts.remove(new CacheIdCount(cacheId, oldCount));

      addedToCache = consolidate(cacheId, value);
    } finally {
      cleanupLock.readLock().unlock();
    }

    if (cleanupStrategy.executeCleanup())
      intermediaryCleanup();

    return addedToCache;
  }

  /**
   * Consolidates the {@link #caches} based on current values of {@link #topCounts}, {@link #memoryConsumption},
   * {@link #flaggedChacheIds} and {@link #currentlyCachedCacheIds}. {@link #memoryConsumption} needs to contain values
   * for all elements in {@link #topCounts}.
   * 
   * <p>
   * This method optionally adds a new element to the cache if eligible.
   * 
   * <p>
   * This method must only be called, if {@link #cleanupLock}s readLock is acquired already!
   * 
   * @param addCacheId
   *          The ID of the element to add or <code>null</code> if nothing should be added.
   * @param addValue
   *          The value of the element to add or <code>null</code> if nothing should be added.
   * @return <code>true</code> in case the value was added successfully.
   */
  private boolean consolidate(CacheId addCacheId, V addValue) {
    boolean addedToCache = false;

    // we retry to identify if we have to add the new value/what other values to remove to/from the cache. This is
    // because while inspecting the situation we might have multiple threads doing the same simultaneously, even with
    // the same CacheId! There we find a decision what we'd like to do, then enter a sync-block and validate if the data
    // we based our decision on is still valid and only if it is, we execute our decision.
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
      Set<CacheId> cacheIdsToBeRemovedFromCache =
          new HashSet<>(Sets.difference(curCurrentlyCachedCacheIds, cacheIdsThatShouldBeCached));

      boolean shouldAddNewCacheIdToCache = addCacheId != null && !curCurrentlyCachedCacheIds.contains(addCacheId)
          && (cacheIdsThatShouldBeCached.contains(addCacheId) || flaggedChacheIds.containsKey(addCacheId));

      if (!cacheIdsToBeRemovedFromCache.isEmpty()
          || (addCacheId != null && cacheIdsThatShouldBeCached.contains(addCacheId))) {
        synchronized (updateCacheSync) {
          if (!curCurrentlyCachedCacheIds.equals(currentlyCachedCacheIds)
              || !DiqubeIterables.startsWith(topCounts, curInspectedCountCacheIds))
            // retry as the data structures we based our decisions on have changed.
            continue;

          // do not remove flagged cache Ids.
          Sets.difference(cacheIdsToBeRemovedFromCache, flaggedChacheIds.keySet()).forEach(id -> removeFromCache(id));

          if (addCacheId != null && shouldAddNewCacheIdToCache) {
            addToCache(addCacheId, addValue);
            addedToCache = true;
          }

          // we succeeded!
          retry = false;
        }
      } else
        // we do not need to take any action, so we're done!
        retry = false;
    }

    return addedToCache;
  }

  /**
   * Consolidate the cache.
   * 
   * <p>
   * This will evict all elements that are not flagged any more and perhaps execute internal cleanup (at the discretion
   * of the cache).
   */
  public void consolidate() {
    removeOldFlaggedCacheIds();

    cleanupLock.readLock().lock();
    try {
      consolidate(null, null);
    } finally {
      cleanupLock.readLock().unlock();
    }

    if (cleanupStrategy.executeCleanup())
      intermediaryCleanup();
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

      // check if we should clean up any "counts"
      Set<? extends Pair<K1, K2>> countCleanups = countCleanupStrategy
          .getCountsForCleanup(Sets.difference(counts.keySet(), currentlyCachedCacheIds), counts.keySet());
      if (countCleanups != null && !countCleanups.isEmpty()) {
        for (Pair<K1, K2> p : countCleanups)
          counts.remove(new CacheId(p.getLeft(), p.getRight()));
      }
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

  public Long getCount(K1 key1, K2 key2) {
    AtomicLong l = counts.get(new CacheId(key1, key2));
    if (l == null)
      return null;
    return l.get();
  }

  /**
   * Internal method to check for flagged elements whose timeout has passed - will remove those elements from
   * {@link #flaggedChacheIds} and will cleanup {@link #flaggedTimes}.
   */
  private void removeOldFlaggedCacheIds() {
    Iterator<Entry<Long, CacheId>> cacheIdEntryIt = flaggedTimes.headMap(System.nanoTime()).entrySet().iterator();
    while (cacheIdEntryIt.hasNext()) {
      Entry<Long, CacheId> cacheIdEntry = cacheIdEntryIt.next();

      CacheId cacheId = cacheIdEntry.getValue();
      long timeoutTime = cacheIdEntry.getKey();

      cacheIdEntryIt.remove();

      FlagInfo flagInfo = flaggedChacheIds.get(cacheId);
      // validate that there is a flagInfo and the flagInfo was not updated in the meantime.
      if (flagInfo != null && flagInfo.getNewestTimeoutNanos().get() == timeoutTime) {
        synchronized (flagInfo) {
          // synched on flagInfo itself and re-check that there is not another thread that currently tries to flag this
          // chacheId!
          // If not synced, we could succeed the "if", but before removing the element from flaggedCacheIds, someone
          // changes the newestTimeoutNanos. And then we'd remove the object -> object is not actually flagged!
          if (flagInfo.getNewestTimeoutNanos().get() == timeoutTime) {
            // remove the flaggedCacheId, but do not directly remove the cached value itself - it might still be in the
            // regular topCounts! Let #offer(..) do the cleanup as soon as it is called.
            flaggedChacheIds.remove(cacheId);
          }
        }
      }
    }
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

  /** for tests */
  /* pcakage */ void setCleanupStrategy(InternalCleanupStrategy cleanupStrategy) {
    this.cleanupStrategy = cleanupStrategy;
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

  private static class FlagInfo {
    private AtomicInteger flagCount;
    private AtomicLong newestTimeoutNanos;

    FlagInfo(long timeoutNanos) {
      flagCount = new AtomicInteger(1);
      newestTimeoutNanos = new AtomicLong(timeoutNanos);
    }

    public AtomicInteger getFlagCount() {
      return flagCount;
    }

    public AtomicLong getNewestTimeoutNanos() {
      return newestTimeoutNanos;
    }
  }

  /**
   * Provider of the size of memory a value takes up.
   */
  public static interface MemoryConsumptionProvider<V> {
    /**
     * @return Number of bytes the given value takes up.
     */
    public long getMemoryConsumptionBytes(V value);
  }

  /**
   * Strategy to decide which collected "count" values should be cleaned up.
   * 
   * <p>
   * This is called once-and-then by {@link CountingCache}.
   */
  public static interface CountCleanupStrategy<K1 extends Comparable<K1>, K2 extends Comparable<K2>> {
    /**
     * Identifies which "count" values to be cleaned up.
     * 
     * <p>
     * Note that only "count" values of cache entries which will never again be "offered" to the cache should be cleaned
     * up, otherwise the counting of such cache elements will re-start at 0 (and therefore probably always lose when
     * compared against the currently-cached elements).
     * 
     * @param countsUpForCleanup
     *          A set of (K1,K2) pairs of cache entries which are currently not cached, but of which a "count" is
     *          available.
     * @param allCounts
     *          A set of (K1, K2) pairs of all counts available. Not that if (K1, K2)s that are only available in this
     *          set (and not in countsUpForCleanup), you might remove the counts of currently cached elements which will
     *          lead to these entries being most probably removed from the cache in the next call to
     *          {@link CountingCache#offer(Comparable, Comparable, Object)} etc.
     * @return Those (K1,K2) pairs which should be cleaned up or <code>null</code> if nothing should be cleaned up and
     *         all counts should be kept.
     */
    public Set<? extends Pair<K1, K2>> getCountsForCleanup(Set<? extends Pair<K1, K2>> countsUpForCleanup,
        Set<? extends Pair<K1, K2>> allCounts);
  }

}
