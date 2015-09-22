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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.diqube.data.colshard.ColumnShard;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests {@link DefaultColumnShardCache}.
 *
 * @author Bastian Gloeckle
 */
public class DefaultColumnShardCacheTest {

  @Test
  public void simpleAdditionCleanupAlways() {
    simpleAddition(true);
  }

  @Test
  public void simpleAdditionCleanupNever() {
    simpleAddition(false);
  }

  private void simpleAddition(boolean cleanupAlways) {
    // GIVEN
    DefaultColumnShardCache cache = new DefaultColumnShardCache(100, () -> cleanupAlways);

    // WHEN
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("1", 50));
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 50));
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 50));

    // THEN
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(0)), Arrays.asList("1"),
        "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(1)), Arrays.asList("2"),
        "Expected to get correct cache entrie(s)");
    Assert.assertNotNull(cache.getCachedColumnShard(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.getCachedColumnShard(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.getNumberOfColumnShardsCached(), 2);
  }

  @Test
  public void simpleAdditionWithEvictCleanupAlways() {
    simpleAdditionWithEvict(true);
  }

  @Test
  public void simpleAdditionWithEvictCleanupNever() {
    simpleAdditionWithEvict(false);
  }

  private void simpleAdditionWithEvict(boolean cleanupAlways) {
    // GIVEN
    DefaultColumnShardCache cache = new DefaultColumnShardCache(100, () -> cleanupAlways);

    // WHEN
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("1", 50));
    // count "2" = 2
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 51));
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 51));

    // THEN
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(0)), Arrays.asList(),
        "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(1)), Arrays.asList("2"),
        "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.getCachedColumnShard(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.getCachedColumnShard(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.getNumberOfColumnShardsCached(), 1);
  }

  @Test
  public void firstAdditionTooBigCleanupAlways() {
    firstAdditionTooBig(true);
  }

  @Test
  public void firstAdditionTooBigCleanupNever() {
    firstAdditionTooBig(false);
  }

  private void firstAdditionTooBig(boolean cleanupAlways) {
    // GIVEN
    DefaultColumnShardCache cache = new DefaultColumnShardCache(100, () -> cleanupAlways);

    // WHEN
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("1", 101));
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("2", 99));

    // THEN
    // expected: nothing cached. Because both entries have the same count - the first one is too big, the second is
    // though ordered after the first, so it will not be cached.
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(0)), Arrays.asList(),
        "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(1)), Arrays.asList(),
        "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.getCachedColumnShard(0, "1"), "Expected to get cached result");
    Assert.assertNull(cache.getCachedColumnShard(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.getNumberOfColumnShardsCached(), 0);
  }

  @Test
  public void firstAdditionTooBigSecondMoreCountCleanupAlways() {
    firstAdditionTooBigSecondMoreCount(true);
  }

  @Test
  public void firstAdditionTooBigSecondMoreCountCleanupNever() {
    firstAdditionTooBigSecondMoreCount(false);
  }

  private void firstAdditionTooBigSecondMoreCount(boolean cleanupAlways) {
    // GIVEN
    DefaultColumnShardCache cache = new DefaultColumnShardCache(100, () -> cleanupAlways);

    // WHEN
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("1", 101));
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 99));
    cache.registerUsageOfColumnShardPossiblyCache(1, shard("2", 99));

    // THEN
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(0)), Arrays.asList(),
        "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(1)), Arrays.asList("2"),
        "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.getCachedColumnShard(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.getCachedColumnShard(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.getNumberOfColumnShardsCached(), 1);
  }

  @Test
  public void firstAdditionTooBigSecondMoreCountOnSingleTableShardCleanupAlways() {
    firstAdditionTooBigSecondMoreCountOnSingleTableShard(true);
  }

  @Test
  public void firstAdditionTooBigSecondMoreCountOnSingleTableShardCleanupNever() {
    firstAdditionTooBigSecondMoreCountOnSingleTableShard(false);
  }

  private void firstAdditionTooBigSecondMoreCountOnSingleTableShard(boolean cleanupAlways) {
    // GIVEN
    DefaultColumnShardCache cache = new DefaultColumnShardCache(100, () -> cleanupAlways);

    // WHEN
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("1", 101));
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("2", 99));
    cache.registerUsageOfColumnShardPossiblyCache(0, shard("2", 99));

    // THEN
    Assert.assertEquals(getNames(cache.getAllCachedColumnShards(0)), Arrays.asList("2"),
        "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.getCachedColumnShard(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.getCachedColumnShard(0, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.getNumberOfColumnShardsCached(), 1);
  }

  private ColumnShard shard(String name, long memorySize) {
    ColumnShard res = Mockito.mock(ColumnShard.class);
    Mockito.when(res.getName()).thenReturn(name);
    Mockito.when(res.calculateApproximateSizeInBytes()).thenReturn(memorySize);
    return res;
  }

  private Collection<String> getNames(Collection<ColumnShard> shards) {
    return shards.stream().map(shard -> shard.getName()).collect(Collectors.toList());
  }
}
