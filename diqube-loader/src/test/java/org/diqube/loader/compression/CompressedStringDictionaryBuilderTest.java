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

import org.diqube.data.str.dict.StringDictionary;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link CompressedStringDictionaryBuilder}.
 *
 * @author Bastian Gloeckle
 */
public class CompressedStringDictionaryBuilderTest {
  private CompressedStringDictionaryBuilder builder;

  @BeforeMethod
  public void before() {
    builder = new CompressedStringDictionaryBuilder();
  }

  @Test
  public void simpleDictTest() {
    // GIVEN
    NavigableMap<String, Long> input = new TreeMap<>();
    input.put("a", 0L);
    input.put("b", 2L);
    input.put("c", 1L);

    // WHEN
    Pair<StringDictionary, Map<Long, Long>> res = builder.fromEntityMap(input).build();
    Map<Long, Long> idMap = res.getRight();
    StringDictionary dict = res.getLeft();

    // THEN
    Map<Long, Long> expectedId = new HashMap<>();
    expectedId.put(2L, 1L);
    expectedId.put(1L, 2L);
    Assert.assertEquals(idMap, expectedId);

    assertTuple(dict, 0, "a");
    assertTuple(dict, 1, "b");
    assertTuple(dict, 2, "c");
  }

  @Test
  public void simpleHierarchicalDictTest() {
    // GIVEN
    NavigableMap<String, Long> input = new TreeMap<>();
    input.put("a", 0L);
    input.put("ab", 1L);
    input.put("ac", 2L);

    // WHEN
    Pair<StringDictionary, Map<Long, Long>> res = builder.fromEntityMap(input).build();
    Map<Long, Long> idMap = res.getRight();
    StringDictionary dict = res.getLeft();

    // THEN
    Map<Long, Long> expectedId = new HashMap<>();
    Assert.assertEquals(idMap, expectedId);

    assertTuple(dict, 0, "a");
    assertTuple(dict, 1, "ab");
    assertTuple(dict, 2, "ac");
  }

  private void assertTuple(StringDictionary dict, long id, String value) {
    Assert.assertEquals(dict.decompressValue(id), value, "Correct value/id tuples expected");
    Assert.assertEquals(dict.findIdOfValue(value), id, "Correct value/id tuples expected");
  }
}
