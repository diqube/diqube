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
package org.diqube.execution;

import java.util.Deque;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import javax.annotation.PostConstruct;

import org.diqube.cache.CountingCache;
import org.diqube.cache.CountingCache.CountCleanupStrategy;
import org.diqube.cache.CountingCache.MemoryConsumptionProvider;
import org.diqube.cache.FlaggingCache;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.flatten.Flattener;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Manages various {@link FlattenedTable}s that are available locally.
 * 
 * <p>
 * Flattened tables that are not needed any more/are not used often enough/exceed a memory cap will be evicted . The
 * implementation is based on a {@link CountingCache}.
 * 
 * <p>
 * The counts are basically calls to {@link #getFlattenedTable(UUID, String, String)}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenedTableManager {
  private static final Logger logger = LoggerFactory.getLogger(FlattenedTableManager.class);

  /**
   * Number of seconds a flattened table gets flagged by
   * {@link #getNewestFlattenedTableVersionAndFlagIt(String, String)} and therefore saves it from being evicted.
   */
  public static final long FLATTENED_TABLE_FLAG_NANOSECONDS = 120 * 1_000_000_000L; /* 2 min */

  private long flagNanoseconds = FLATTENED_TABLE_FLAG_NANOSECONDS;

  /**
   * Execute cache consolidation in approx. 10% of cases.
   */
  private CacheConsolidateStrategy cacheConsolidateStrategy = () -> ThreadLocalRandom.current().nextInt(128) < 13;

  @Config(ConfigKey.FLATTEN_CACHE_SIZE_MB)
  private int flattenedTableCacheSizeMb;

  /**
   * {@link FlaggingCache} we use to store the {@link FlattenedTable}s.
   * 
   * <p>
   * Key1: Pair of tableName, flattenBy <br/>
   * Key2: versionId.
   * 
   * <p>
   * This cache typically holds only the newest versions of the FlattenedTables, although old versions might still be
   * available if they were flagged.
   */
  private CountingCache<Pair<String, String>, UUID, FlattenedTableInfo> cache;

  /**
   * Usage counts for table/flatten-by pairs.
   * 
   * This field will never be cleaned up, it therefore counts usages even when the corresponding table gets evicted from
   * {@link #cache} in the meantime (the counts inside the cache get evicted, because the cache key is based on the
   * version Id additionally).
   */
  private ConcurrentMap<Pair<String, String>, AtomicLong> usageCounts = new ConcurrentHashMap<>();

  /**
   * ID of the newest version of a table/flatten-by pair. Sync access to this and cache by the UUID value of this map.
   */
  private ConcurrentMap<Pair<String, String>, UUID> newestVersionIds = new ConcurrentHashMap<>();

  /** Elements whose counts should be removed in {@link #cache} on the next opportunity. */
  private Deque<Pair<Pair<String, String>, UUID>> countCleanupCacheEntries = new ConcurrentLinkedDeque<>();

  @PostConstruct
  public void initialize() {
    // Use a CountCleanupStrategy that cleans up everything that was already evicted from the cache: If something was
    // evicted from the cache, we definitely
    // won't offer it again, since we will not use that same versionId again. Therefore we can free up the count memory
    // of those.
    // Additionally we remove the counts of every version that is in #countCleanupCacheEntries. These are old versions.
    // If anybody still needs those versions, they must have flagged those elements in the cache, otherwise their
    // entries will have count 0 and that will most probably lead to them being evicted from the cache on the next run.
    CountCleanupStrategy<Pair<String, String>, UUID> cacheCountCleanupStrategy = (countsForCleanup, allCounts) -> {
      Set<Pair<Pair<String, String>, UUID>> curCountCleanupCacheEntries = new HashSet<>();
      while (!countCleanupCacheEntries.isEmpty()) {
        try {
          curCountCleanupCacheEntries.add(countCleanupCacheEntries.pop());
        } catch (NoSuchElementException e) {
          // swallow -> two thread concurrently traversed countCleanupCacheEntries and our thread did not get another
          // element. Thats fine. (Although this will not happen currently, since CountingCache synchronizes).
        }
      }

      Set<Pair<Pair<String, String>, UUID>> res =
          Sets.union(countsForCleanup, Sets.intersection(allCounts, curCountCleanupCacheEntries));
      logger.trace("Evicting old usage counts (limit): {}", Iterables.limit(res, 100));
      return res;
    };

    MemoryConsumptionProvider<FlattenedTableInfo> cacheMemoryConsumptionProvider =
        info -> info.getFlattenedTable().calculateApproximateSizeInBytes();

    cache = new CountingCache<>(flattenedTableCacheSizeMb * 1024L * 1024L, cacheMemoryConsumptionProvider,
        cacheCountCleanupStrategy);
  }

  /**
   * Register a newly created {@link FlattenedTable} from {@link Flattener}.
   * 
   * <p>
   * This version will automatically be the newest version available, so it is likely that
   * {@link #getNewestFlattenedTableVersion(String, String)} will return this flattened table version if called right
   * after registering the new version.
   * 
   * <p>
   * The new flattenedTable will be available through this table manager at least for
   * {@link #FLATTENED_TABLE_FLAG_NANOSECONDS}.
   * 
   * @param versionId
   *          The version ID of the flattened table.
   * @param flattenedTable
   *          The flattened table itself.
   * @param origTableName
   *          The table the flattened table was based on.
   * @param flattenBy
   *          The field which the table was flattened by.
   */
  public void registerFlattenedTableVersion(UUID versionId, FlattenedTable flattenedTable, String origTableName,
      String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);

    // We manually manage the "counts" here: Get the count from usageCounts. Then manually force the cache to remove the
    // count on the old version (which will lead to eviction of the old entity from the cache).
    // This counting is not 100% thread safe, as we might loose a few "counts" if this method is called simultaneously
    // for the same flattening with different flattenedTables - but that is not a big problem, since typically only one
    // Flattener will be running for one table/flatten-by pair anyway.
    FlattenedTableInfo newInfo = new FlattenedTableInfo(versionId, flattenedTable);

    Holder<Long> oldCountHolder = new Holder<>(null);

    Runnable update = () -> {
      // flag new flattened table to make sure it definitely ends up being in the cache.

      if (oldCountHolder.getValue() != null)
        cache.offerAndFlag(keyPair, versionId, newInfo, System.nanoTime() + flagNanoseconds,
            oldCountHolder.getValue() + 1L);
      else
        cache.offerAndFlag(keyPair, versionId, newInfo, System.nanoTime() + flagNanoseconds);

      usageCounts.merge(keyPair, new AtomicLong(1L), (oldValue, newValue) -> {
        AtomicLong res = new AtomicLong(oldValue.get());
        res.addAndGet(newValue.get());
        return res;
      });

      newestVersionIds.put(keyPair, versionId);
    };

    Pair<UUID, FlattenedTable> oldNewestVersionPair = getNewestFlattenedTableVersion(origTableName, flattenBy);
    if (oldNewestVersionPair != null && !oldNewestVersionPair.getLeft().equals(versionId)) {
      // sync on previous UUID, since that'd be used by anyone who wants to query the newsest UUID.
      synchronized (oldNewestVersionPair.getLeft()) {
        Long c = usageCounts.get(keyPair).get();
        if (c != null)
          oldCountHolder.setValue(c);

        logger.info(
            "Registering new flattened table {} from table '{}' flattened by '{}' of which an "
                + "old version was in the cache already ({}). Using cached usageCounts for new table: {}",
            versionId, origTableName, flattenBy, oldNewestVersionPair.getLeft(), oldCountHolder.getValue());

        update.run();
      }

      countCleanupCacheEntries.add(new Pair<>(keyPair, oldNewestVersionPair.getLeft()));
    } else {
      logger.info("Registering new flattened table {} from table '{}' flattened by '{}'", versionId, origTableName,
          flattenBy);
      update.run();
    }
  }

  /**
   * Fetches the newest version of a flattened table.
   * 
   * @param origTableName
   *          Name of the original table.
   * @param flattenBy
   *          Field by which the orig table was flattened.
   * @return Pair of version ID and flattened table, or <code>null</code> in case there is no flattened version of that
   *         table.
   */
  public Pair<UUID, FlattenedTable> getNewestFlattenedTableVersion(String origTableName, String flattenBy) {
    logger.trace("Getting newest version of flattened table '{}' by '{}'", origTableName, flattenBy);
    return getNewestFlattenedTableVersion(origTableName, flattenBy,
        (keyPair, newestVersion) -> cache.get(keyPair, newestVersion));
  }

  /**
   * Fetches the newest version of a flattened table and flag that version to not be evicted for
   * {@link #FLATTENED_TABLE_FLAG_NANOSECONDS}.
   * 
   * @param origTableName
   *          Name of the original table.
   * @param flattenBy
   *          Field by which the orig table was flattened.
   * @return Pair of version ID and flattened table, or <code>null</code> in case there is no flattened version of that
   *         table.
   */
  public Pair<UUID, FlattenedTable> getNewestFlattenedTableVersionAndFlagIt(String origTableName, String flattenBy) {
    logger.trace("Flagging and getting newest version of flattened table '{}' by '{}'", origTableName, flattenBy);
    return getNewestFlattenedTableVersion(origTableName, flattenBy, //
        (keyPair, newestVersion) -> //
        cache.flagAndGet(keyPair, newestVersion, System.nanoTime() + flagNanoseconds));
  }

  private Pair<UUID, FlattenedTable> getNewestFlattenedTableVersion(String origTableName, String flattenBy,
      BiFunction<Pair<String, String>, UUID, FlattenedTableInfo> flattenTableInfoResolver) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);

    UUID newestVersion = newestVersionIds.get(keyPair);
    while (newestVersion != null) {
      FlattenedTableInfo info = null;
      synchronized (newestVersion) {
        UUID newNewestVersion = newestVersionIds.get(keyPair);
        if (!newestVersion.equals(newNewestVersion)) {
          // newestVersion changed, we have synced on old object -> retry!
          newestVersion = newNewestVersion;
          continue;
        }
        info = flattenTableInfoResolver.apply(keyPair, newestVersion);
      }

      if (info != null)
        return new Pair<>(newestVersion, info.getFlattenedTable());
      else
        return null;
    }

    return null;
  }

  /**
   * Get a specific version of a flattened table and increase its usage count.
   * 
   * @param versionId
   *          The version of the flattened table.
   * @param origTableName
   *          The table name the flattening was based on.
   * @param flattenBy
   *          The field by which the original table was flattened.
   * @return The {@link FlattenedTable} or <code>null</code> if it is not available.
   */
  public FlattenedTable getFlattenedTable(UUID versionId, String origTableName, String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    FlattenedTableInfo info = cache.get(keyPair, versionId);

    if (info != null) {
      logger.trace("Using version {} of flattened table '{}' by '{}'.", versionId, origTableName, flattenBy);

      // increase usage count in cache and in our cache-evict-safe map.
      // Note that this will also happen if a version of a flattened table is fetched which is about to be removed from
      // the cache (= whose count was/will be removed). This is not that nice, since old versions get another "used"
      // count although they should have been deleted right away, but there is no simple way around this unfortunately.
      // The old versions might get another count, but that is not as bad either, as the count will always be very low
      // and the entry will be evicted soon again (together with its count being removed).
      cache.offer(keyPair, versionId, info);
      usageCounts.get(keyPair).incrementAndGet();

      // Consolidate cache. This will evict all not-any-more-flagged entries. We need to do this, as we might not call
      // #offer on the cache that often (which would execute the same). But if we do not do this here, we might end up
      // leaving unneeded objects in the cache for a longer time.
      if (cacheConsolidateStrategy.consolidateCache())
        cache.consolidate();

      return info.getFlattenedTable();
    }

    // Consolidate cache.
    if (cacheConsolidateStrategy.consolidateCache())
      cache.consolidate();

    return null;
  }

  /** for tests */
  /* package */ void setFlagNanoseconds(long flagNanoseconds) {
    this.flagNanoseconds = flagNanoseconds;
  }

  /** for tests */
  /* package */ void setFlattenedTableCacheSizeMb(int flattenedTableCacheSizeMb) {
    this.flattenedTableCacheSizeMb = flattenedTableCacheSizeMb;
    initialize();
  }

  /** for tests */
  /* package */ CountingCache<Pair<String, String>, UUID, FlattenedTableInfo> getCache() {
    return cache;
  }

  /** for tests */
  /* package */void setCacheConsolidateStrategy(CacheConsolidateStrategy cacheConsolidateStrategy) {
    this.cacheConsolidateStrategy = cacheConsolidateStrategy;
  }

  private class FlattenedTableInfo {
    private UUID versionId;
    private FlattenedTable flattenedTable;

    public FlattenedTableInfo(UUID versionId, FlattenedTable flattenedTable) {
      this.versionId = versionId;
      this.flattenedTable = flattenedTable;
    }

    public UUID getVersionId() {
      return versionId;
    }

    public FlattenedTable getFlattenedTable() {
      return flattenedTable;
    }
  }

  /* package */ static interface CacheConsolidateStrategy {
    public boolean consolidateCache();
  }

}
