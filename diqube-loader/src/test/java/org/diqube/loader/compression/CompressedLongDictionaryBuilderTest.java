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
package org.diqube.loader.compression;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class CompressedLongDictionaryBuilderTest {
  private CompressedLongDictionaryBuilder builder;

  @BeforeMethod
  public void before() {
    builder = new CompressedLongDictionaryBuilder();
  }

  @Test
  public void idsEqualBitEfficientTest() {
    // GIVEN
    // Values that have correct IDs already and which should be compressed using the BitEfficient strategy
    NavigableMap<Long, Long> values = new TreeMap<>();
    values.put(Long.MIN_VALUE, 0L);
    values.put(0L, 1L);
    values.put(10L, 2L);

    // WHEN
    Pair<LongDictionary<?>, Map<Long, Long>> res = builder.fromEntityMap(values).build();
    LongDictionary<?> dict = res.getLeft();

    // THEN
    Assert.assertEquals(res.getRight(), new HashMap<Long, Long>(), "Expected no ID changes");
    assertTuple(dict, 0L, Long.MIN_VALUE);
    assertTuple(dict, 1L, 0L);
    assertTuple(dict, 2L, 10L);
  }

  @Test
  public void idsEqualRefBasedTest() {
    // GIVEN
    // Values that have correct IDs already and which should be compressed using the RefBasedstrategy
    NavigableMap<Long, Long> values = new TreeMap<>();
    values.put(1000L, 0L);
    values.put(1001L, 1L);
    values.put(1002L, 2L);

    // WHEN
    Pair<LongDictionary<?>, Map<Long, Long>> res = builder.fromEntityMap(values).build();
    LongDictionary<?> dict = res.getLeft();

    // THEN
    Assert.assertEquals(res.getRight(), new HashMap<Long, Long>(), "Expected no ID changes");
    assertTuple(dict, 0L, 1000L);
    assertTuple(dict, 1L, 1001L);
    assertTuple(dict, 2L, 1002L);
  }

  @Test
  public void idsDifferentRefBasedTest() {
    // GIVEN
    // Values that have correct IDs already and which should be compressed using the RefBasedstrategy
    NavigableMap<Long, Long> values = new TreeMap<>();
    values.put(1000L, 2L);
    values.put(1001L, 1L);
    values.put(1002L, 0L);

    // WHEN
    Pair<LongDictionary<?>, Map<Long, Long>> res = builder.fromEntityMap(values).build();
    LongDictionary<?> dict = res.getLeft();

    // THEN
    Map<Long, Long> expectedId = new HashMap<Long, Long>();
    expectedId.put(2L, 0L);
    expectedId.put(0L, 2L);
    Assert.assertEquals(res.getRight(), expectedId, "Expected specific ID changes");
    assertTuple(dict, 0L, 1000L);
    assertTuple(dict, 1L, 1001L);
    assertTuple(dict, 2L, 1002L);
  }

  @Test
  public void idsDifferentBitEfficientTest() {
    // GIVEN
    // Values that have correct IDs already and which should be compressed using the RefBasedstrategy
    NavigableMap<Long, Long> values = new TreeMap<>();
    values.put(Long.MIN_VALUE, 2L);
    values.put(0L, 1L);
    values.put(10L, 0L);

    // WHEN
    Pair<LongDictionary<?>, Map<Long, Long>> res = builder.fromEntityMap(values).build();
    LongDictionary<?> dict = res.getLeft();

    // THEN
    Map<Long, Long> expectedId = new HashMap<Long, Long>();
    expectedId.put(2L, 0L);
    expectedId.put(0L, 2L);
    Assert.assertEquals(res.getRight(), expectedId, "Expected specific ID changes");
    assertTuple(dict, 0L, Long.MIN_VALUE);
    assertTuple(dict, 1L, 0L);
    assertTuple(dict, 2L, 10L);
  }

  private void assertTuple(LongDictionary<?> dict, long id, long value) {
    Assert.assertEquals((long) dict.decompressValue(id), value, "Correct value/id tuples expected");
    Assert.assertEquals(dict.findIdOfValue(value), id, "Correct value/id tuples expected");
  }
}
