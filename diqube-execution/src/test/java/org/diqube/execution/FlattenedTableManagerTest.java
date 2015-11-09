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

import java.util.UUID;

import org.diqube.cache.CountingCacheTestUtil;
import org.diqube.context.Profiles;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.util.Pair;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link FlattenedTableManager}.
 *
 * @author Bastian Gloeckle
 */
public class FlattenedTableManagerTest {
  private static final int MEMORY_CAP_CACHE_MB = 5;
  private static final String TABLE = "tab";
  private static final String FLATTEN_BY = "a[*]";

  private AnnotationConfigApplicationContext dataContext;

  private FlattenedTableManager flattenedTableManager;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    flattenedTableManager = dataContext.getBean(FlattenedTableManager.class);
    flattenedTableManager.setFlattenedTableCacheSizeMb(MEMORY_CAP_CACHE_MB);
    flattenedTableManager.initialize();

    // force the internal cache to do the internal cleanup /always/.
    CountingCacheTestUtil.setCleanupStrategy(flattenedTableManager.getCache(), () -> true);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void oldCountGetsAddedToNewVersionAndOldVersionGetsEvicted() {
    // GIVEN
    FlattenedTable oldTable = table(MEMORY_CAP_CACHE_MB);
    UUID oldUuid = UUID.randomUUID();
    FlattenedTable newTable = table(MEMORY_CAP_CACHE_MB);
    UUID newUuid = UUID.randomUUID();

    flattenedTableManager.setFlagNanoseconds(0L); // no automatic flagging in register call
    flattenedTableManager.setCacheConsolidateStrategy(() -> false); // never consolidate automatically, we trigger this
                                                                    // manually.

    // WHEN
    flattenedTableManager.registerFlattenedTableVersion(oldUuid, oldTable, TABLE, FLATTEN_BY);
    flattenedTableManager.registerFlattenedTableVersion(newUuid, newTable, TABLE, FLATTEN_BY);

    flattenedTableManager.getCache().consolidate();

    // THEN
    Assert.assertEquals(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getLeft(), newUuid,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getRight() == newTable,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(newUuid, TABLE, FLATTEN_BY) == newTable,
        "Expected that newest table is returned when querying.");

    Assert.assertNull(flattenedTableManager.getFlattenedTable(oldUuid, TABLE, FLATTEN_BY),
        "Expected that old table is evicted.");

    Pair<String, String> keyPair = new Pair<>(TABLE, FLATTEN_BY);

    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, newUuid), 3L /* 2 register, 1 get */,
        "Expected correct count for new version");

    // null: 1 registered (removed when new version was registered), 1 get (did not succeed because element was fully
    // evicted already).
    Assert.assertNull(flattenedTableManager.getCache().getCount(keyPair, oldUuid),
        "Expected correct count for old version");
  }

  @Test
  public void registeringFlagsTable() {
    // GIVEN
    FlattenedTable oldTable = table(MEMORY_CAP_CACHE_MB);
    UUID oldUuid = UUID.randomUUID();
    FlattenedTable newTable = table(MEMORY_CAP_CACHE_MB);
    UUID newUuid = UUID.randomUUID();

    flattenedTableManager.setFlagNanoseconds(10 * 1_000_000_000L); // 10s
    flattenedTableManager.setCacheConsolidateStrategy(() -> false); // never consolidate automatically, we trigger this
                                                                    // manually.

    // WHEN
    flattenedTableManager.registerFlattenedTableVersion(oldUuid, oldTable, TABLE, FLATTEN_BY);
    flattenedTableManager.registerFlattenedTableVersion(newUuid, newTable, TABLE, FLATTEN_BY);

    flattenedTableManager.getCache().consolidate();

    // THEN
    Assert.assertEquals(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getLeft(), newUuid,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getRight() == newTable,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(newUuid, TABLE, FLATTEN_BY) == newTable,
        "Expected that newest table is returned when querying.");
    // oldTable should still be returned, since it should be flagged.
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(oldUuid, TABLE, FLATTEN_BY) == oldTable,
        "Expected that old table is returned when querying.");

    Pair<String, String> keyPair = new Pair<>(TABLE, FLATTEN_BY);

    flattenedTableManager.getCache().consolidate();

    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, newUuid), 3L /* 2 register, 1 get */,
        "Expected correct count for new version");
    // oldtable should not have a count anymore -> It will be evicted after its flagging time is over.
    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, oldUuid),
        1L /* 1 register (removed when new version was registered), 1 get */, "Expected correct count for old version");
  }

  @Test
  public void newestFlagFlags() throws InterruptedException {
    // GIVEN
    FlattenedTable oldTable = table(MEMORY_CAP_CACHE_MB);
    UUID oldUuid = UUID.randomUUID();
    FlattenedTable newTable = table(MEMORY_CAP_CACHE_MB);
    UUID newUuid = UUID.randomUUID();

    flattenedTableManager.setFlagNanoseconds(2 * 1_000_000_000L); // 2s
    flattenedTableManager.setCacheConsolidateStrategy(() -> false); // never consolidate automatically, we trigger this
                                                                    // manually.

    // WHEN
    flattenedTableManager.registerFlattenedTableVersion(oldUuid, oldTable, TABLE, FLATTEN_BY);
    Thread.sleep(1500);
    Pair<UUID, FlattenedTable> newestReturn =
        flattenedTableManager.getNewestFlattenedTableVersionAndFlagIt(TABLE, FLATTEN_BY);
    flattenedTableManager.registerFlattenedTableVersion(newUuid, newTable, TABLE, FLATTEN_BY);
    Thread.sleep(1000); // if only "register" flagged the entry, that flag is now outdated. But if "newest" flagged it,
                        // too, the flag is still valid.

    flattenedTableManager.getCache().consolidate();

    // THEN
    Assert.assertEquals(newestReturn.getLeft(), oldUuid, "The newest method should've returned the correct ID.");
    Assert.assertTrue(newestReturn.getRight() == oldTable, "The newest method should've returned the correct table.");

    Assert.assertEquals(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getLeft(), newUuid,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getRight() == newTable,
        "Expected that new table is newest one");
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(newUuid, TABLE, FLATTEN_BY) == newTable,
        "Expected that newest table is returned when querying.");
    // oldTable should still be returned, since it should be flagged.
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(oldUuid, TABLE, FLATTEN_BY) == oldTable,
        "Expected that old table is returned when querying.");

    Pair<String, String> keyPair = new Pair<>(TABLE, FLATTEN_BY);

    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, newUuid), 3/* 2 register, 1 get */,
        "Expected correct count for new version");
    // oldtable should not have a count anymore -> It will be evicted after its flagging time is over.
    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, oldUuid),
        1 /* 1 register (which was removed when the new version was registered), 1 get */,
        "Expected correct count for old version");
  }

  @Test
  public void registerTheSameTwice() throws InterruptedException {
    // GIVEN
    FlattenedTable oldTable = table(MEMORY_CAP_CACHE_MB);
    FlattenedTable newTable = table(MEMORY_CAP_CACHE_MB);
    UUID uuid = UUID.randomUUID();

    flattenedTableManager.setFlagNanoseconds(2 * 1_000_000_000L); // 2s
    flattenedTableManager.setCacheConsolidateStrategy(() -> false); // never consolidate automatically, we trigger this
                                                                    // manually.

    // WHEN
    // use same UUID, but different table instances so we can disinguish later.
    flattenedTableManager.registerFlattenedTableVersion(uuid, oldTable, TABLE, FLATTEN_BY);
    flattenedTableManager.registerFlattenedTableVersion(uuid, newTable, TABLE, FLATTEN_BY);

    flattenedTableManager.getCache().consolidate();

    // THEN
    Assert.assertEquals(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getLeft(), uuid,
        "Expected that valid table is newest one");
    Assert.assertTrue(flattenedTableManager.getNewestFlattenedTableVersion(TABLE, FLATTEN_BY).getRight() == oldTable,
        "Expected that valid table is newest one");
    Assert.assertTrue(flattenedTableManager.getFlattenedTable(uuid, TABLE, FLATTEN_BY) == oldTable,
        "Expected that old table is returned when querying.");

    Pair<String, String> keyPair = new Pair<>(TABLE, FLATTEN_BY);

    // this will fail if e.g. the entry has falsely been marked for eviction.
    Assert.assertEquals((long) flattenedTableManager.getCache().getCount(keyPair, uuid), 3L/* two register, one get */,
        "Expected correct count");
  }

  private FlattenedTable table(long memoryMB) {
    FlattenedTable mockedTable = Mockito.mock(FlattenedTable.class);
    Mockito.when(mockedTable.calculateApproximateSizeInBytes()).thenReturn(memoryMB * 1024L * 1024L);
    return mockedTable;
  }
}
