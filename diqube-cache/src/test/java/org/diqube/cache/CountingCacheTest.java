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

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.diqube.cache.CountingCache.MemoryConsumptionProvider;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests {@link CountingCache}.
 *
 * @author Bastian Gloeckle
 */
public class CountingCacheTest {

  private static final MemoryConsumptionProvider<CachedValue> MEM_PROV = (v) -> v.memorySize;

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
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> cleanupAlways, MEM_PROV);

    // WHEN
    cache.offer(0, "1", value("1", 50));
    cache.offer(1, "2", value("2", 50));
    cache.offer(1, "2", value("2", 50));

    // THEN
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList("1"), "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAll(1)), Arrays.asList("2"), "Expected to get correct cache entrie(s)");
    Assert.assertNotNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.get(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 2);
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
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> cleanupAlways, MEM_PROV);

    // WHEN
    cache.offer(0, "1", value("1", 50));
    // count "2" = 2
    cache.offer(1, "2", value("2", 51));
    cache.offer(1, "2", value("2", 51));

    // THEN
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList(), "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAll(1)), Arrays.asList("2"), "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.get(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 1);
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
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> cleanupAlways, MEM_PROV);

    // WHEN
    cache.offer(0, "1", value("1", 101));
    cache.offer(0, "2", value("2", 99));

    // THEN
    // expected: nothing cached. Because both entries have the same count - the first one is too big, the second is
    // though ordered after the first, so it will not be cached.
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList(), "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAll(1)), Arrays.asList(), "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNull(cache.get(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 0);
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
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> cleanupAlways, MEM_PROV);

    // WHEN
    cache.offer(0, "1", value("1", 101));
    cache.offer(1, "2", value("2", 99));
    cache.offer(1, "2", value("2", 99));

    // THEN
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList(), "Expected to get correct cache entrie(s)");
    Assert.assertEquals(getNames(cache.getAll(1)), Arrays.asList("2"), "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.get(1, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 1);
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
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> cleanupAlways, MEM_PROV);

    // WHEN
    cache.offer(0, "1", value("1", 101));
    cache.offer(0, "2", value("2", 99));
    cache.offer(0, "2", value("2", 99));

    // THEN
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList("2"), "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNotNull(cache.get(0, "2"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 1);
  }

  @Test
  public void countsDoNotGetLost() {
    // GIVEN
    CountingCache<Integer, String, CachedValue> cache = new CountingCache<>(100, () -> true, MEM_PROV);

    // cleanup each time, internal cleanup should NOT remove counts when adding "3".

    // WHEN
    cache.offer(0, "1", value("1", 99));
    cache.offer(0, "1", value("1", 99));
    cache.offer(0, "2", value("2", 99));
    cache.offer(0, "2", value("2", 99));
    cache.offer(0, "3", value("3", 99));
    cache.offer(0, "3", value("3", 99));
    cache.offer(0, "3", value("3", 99));

    // THEN
    Assert.assertEquals(getNames(cache.getAll(0)), Arrays.asList("3"), "Expected to get correct cache entrie(s)");
    Assert.assertNull(cache.get(0, "1"), "Expected to get cached result");
    Assert.assertNull(cache.get(0, "2"), "Expected to get cached result");
    Assert.assertNotNull(cache.get(0, "3"), "Expected to get cached result");
    Assert.assertEquals(cache.size(), 1);
  }

  private CachedValue value(String name, long memorySize) {
    CachedValue res = new CachedValue();
    res.name = name;
    res.memorySize = memorySize;
    return res;
  }

  private Collection<String> getNames(Collection<CachedValue> shards) {
    return shards.stream().map(shard -> shard.name).collect(Collectors.toList());
  }

  private static class CachedValue {
    String name;
    long memorySize;
  }
}
