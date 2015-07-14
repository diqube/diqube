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
package org.diqube.data.lng.compression;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import org.diqube.data.lng.array.BitEfficientLongArray;
import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.lng.dict.ConstantLongDictionary;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.HashBiMap;

/**
 * Tests {@link ArrayCompressedLongDictionary}.
 *
 * @author Bastian Gloeckle
 */
public class ArrayCompressedLongDictionaryTest {
  private static final long ID_VALUE_DELTA = 1000L;
  private static final long MAX_ID = 1000;

  private ArrayCompressedLongDictionary dict;

  @BeforeMethod
  public void before() {
    CompressedLongArray array =
        new BitEfficientLongArray(LongStream.range(ID_VALUE_DELTA, ID_VALUE_DELTA + MAX_ID + 1).toArray(), true);

    dict = new ArrayCompressedLongDictionary(array);
  }

  @Test
  public void firstEntry() {
    Assert.assertEquals((long) dict.decompressValue(0L), 0L + ID_VALUE_DELTA);
    Assert.assertEquals(dict.findIdOfValue(0L + ID_VALUE_DELTA), 0L);
  }

  @Test
  public void lastEntry() {
    Assert.assertEquals((long) dict.decompressValue(MAX_ID), MAX_ID + ID_VALUE_DELTA);
    Assert.assertEquals(dict.findIdOfValue(MAX_ID + ID_VALUE_DELTA), MAX_ID);
  }

  @Test
  public void middleEntry() {
    long id = MAX_ID / 2;
    long value = id + ID_VALUE_DELTA;
    Assert.assertEquals((long) dict.decompressValue(id), value);
    Assert.assertEquals(dict.findIdOfValue(value), id);
  }

  @Test
  public void twoThridsEntry() {
    long id = (MAX_ID * 2) / 3;
    long value = id + ID_VALUE_DELTA;
    Assert.assertEquals((long) dict.decompressValue(id), value);
    Assert.assertEquals(dict.findIdOfValue(value), id);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void decompressInvalid() {
    dict.decompressValue(MAX_ID + 1);
  }

  @Test
  public void containsAnyValueTest() {
    Assert.assertTrue(dict.containsAnyValue(new Long[] { ID_VALUE_DELTA }));
    Assert.assertFalse(dict.containsAnyValue(new Long[] { 1L }));
    Assert.assertFalse(dict.containsAnyValue(new Long[] { ID_VALUE_DELTA + MAX_ID + 1 }));
    Assert.assertTrue(dict.containsAnyValue(new Long[] { ID_VALUE_DELTA + MAX_ID }));
  }

  @Test
  public void decompressMultipleValues() {
    Long[] ids = new Long[] { 0L, 1L, MAX_ID, MAX_ID - 1 };
    Long[] decompressedValues = dict.decompressValues(ids);
    for (int i = 0; i < ids.length; i++)
      Assert.assertEquals(decompressedValues[i], Long.valueOf(ids[i] + ID_VALUE_DELTA),
          "Value for ID " + ids[i] + " is wrong");
  }

  @Test
  public void idsOfMulipleValues() {
    Long[] values =
        new Long[] { ID_VALUE_DELTA, ID_VALUE_DELTA + 1, ID_VALUE_DELTA + MAX_ID - 1, ID_VALUE_DELTA + MAX_ID };
    Long[] ids = dict.findIdsOfValues(values);
    Long[] expectedIds = { 0L, 1L, MAX_ID - 1, MAX_ID };
    Assert.assertEquals(ids, expectedIds);
  }

  @Test
  public void ltEq1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L); // array1[0] <= array2[i] for i>=0

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 6 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L); // array1[0] <= array2[i] for i>=0
    expected.put(1L, -4L); // array1[1] <= array2[i] for i>=3

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L); // array1[0] <= array2[i] for i>=0
    expected.put(1L, 3L); // array1[1] == array2[3], therefore array1[1] < array2[i] for i > 3

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 0, 1, 2, 3, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 1L); // array1[0] == array2[1], therefore array1[0] < array2[i] for i > 1
    expected.put(1L, 4L); // array1[1] == array2[4], therefore array1[1] < array2[i] for i > 4

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 5, 6 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 4 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    // empty,as all elements in array1 > all elements in array2

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq6Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 5, 6 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 4, 7 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -5L); // array1[0] <= array2[i] for i>=4
    expected.put(1L, -5L); // array1[1] <= array2[i] for i>=4

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEq7Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 1 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 2, 3, 4 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> ltEq = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L); // array1[0] <= array2[i] for i>=0
    expected.put(1L, -1L); // array1[1] <= array2[i] for i>=0

    Assert.assertEquals(ltEq, expected);
  }

  @Test
  public void ltEqValue1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> ltEq = dict1.findIdsOfValuesLtEq(4L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L }));

    Assert.assertEquals(ltEq, expected);
    Assert.assertTrue(dict1.containsAnyValueLtEq(4L));
  }

  @Test
  public void ltEqValue2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> ltEq = dict1.findIdsOfValuesLtEq(5L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L }));

    Assert.assertEquals(ltEq, expected);
    Assert.assertTrue(dict1.containsAnyValueLtEq(5L));
  }

  @Test
  public void ltEqValue3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> ltEq = dict1.findIdsOfValuesLtEq(0L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] {}));

    Assert.assertEquals(ltEq, expected);
    Assert.assertFalse(dict1.containsAnyValueLtEq(0L));
  }

  @Test
  public void ltEqValue4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> ltEq = dict1.findIdsOfValuesLtEq(1L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L }));

    Assert.assertEquals(ltEq, expected);
    Assert.assertTrue(dict1.containsAnyValueLtEq(1L));
  }

  @Test
  public void ltValue1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> lt = dict1.findIdsOfValuesLt(4L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L }));

    Assert.assertEquals(lt, expected);
    Assert.assertTrue(dict1.containsAnyValueLt(4L));
  }

  @Test
  public void ltValue2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> lt = dict1.findIdsOfValuesLt(5L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L }));

    Assert.assertEquals(lt, expected);
    Assert.assertTrue(dict1.containsAnyValueLt(5L));
  }

  @Test
  public void ltValue3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> lt = dict1.findIdsOfValuesLt(0L);

    // THEN
    Set<Long> expected = new HashSet<>();

    Assert.assertEquals(lt, expected);
    Assert.assertFalse(dict1.containsAnyValueLt(0L));
  }

  @Test
  public void ltValue4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> lt = dict1.findIdsOfValuesLt(1L);

    // THEN
    Set<Long> expected = new HashSet<>();

    Assert.assertEquals(lt, expected);
    Assert.assertFalse(dict1.containsAnyValueLt(1L));
  }

  @Test
  public void ltValue5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> lt = dict1.findIdsOfValuesLt(6L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L }));

    Assert.assertEquals(lt, expected);
    Assert.assertTrue(dict1.containsAnyValueLt(6L));
  }

  @Test
  public void gtEq1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, -3L); // array1[1] >= array2[i] for i<=2

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void gtEq2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 6 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, -3L); // array1[1] >= array2[i] for i<=2

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void gtEq3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 3L); // array1[1] == array2[3], therefore array1[1] > array2[i] for i<3

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void gtEq4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 0, 1, 2, 3, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 1L); // array1[0] == array2[1], therefore array1[1] > array2[i] for i<1
    expected.put(1L, 4L); // array1[1] == array2[4], therefore array1[1] > array2[i] for i<4

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void gtEq5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 5, 6 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 4 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -4L); // array1[0] <= array2[i] for i<3
    expected.put(1L, -4L); // array1[1] <= array2[i] for i<3

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void gtEq6Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 5, 6 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 4, 7 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> grEq = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -4L); // array1[0] <= array2[i] for i<3
    expected.put(1L, -4L); // array1[1] <= array2[i] for i<3

    Assert.assertEquals(grEq, expected);
  }

  @Test
  public void eq1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 1, 2, 3, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> eq = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 3L);
    Assert.assertEquals(eq, expected);

    expected = HashBiMap.create(expected).inverse();
    Assert.assertEquals(dict2.findEqualIds(dict1), expected);
  }

  @Test
  public void eq2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 0, 1, 3, 7 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> eq = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 1L);
    Assert.assertEquals(eq, expected);

    expected = HashBiMap.create(expected).inverse();
    Assert.assertEquals(dict2.findEqualIds(dict1), expected);
  }

  @Test
  public void eq3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 0, 1, 5, 7 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> eq = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 1L);
    expected.put(1L, 2L);
    Assert.assertEquals(eq, expected);

    expected = HashBiMap.create(expected).inverse();
    Assert.assertEquals(dict2.findEqualIds(dict1), expected);
  }

  @Test
  public void eq4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5, 8 }, true);
    CompressedLongArray array2 = new BitEfficientLongArray(new long[] { 0, 1, 5, 7 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);
    ArrayCompressedLongDictionary dict2 = new ArrayCompressedLongDictionary(array2);

    // WHEN
    Map<Long, Long> eq = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 1L);
    expected.put(1L, 2L);
    Assert.assertEquals(eq, expected);

    expected = HashBiMap.create(expected).inverse();
    Assert.assertEquals(dict2.findEqualIds(dict1), expected);
  }

  @Test
  public void gtEqValue1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gtEq = dict1.findIdsOfValuesGtEq(4L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 1L }));

    Assert.assertEquals(gtEq, expected);
    Assert.assertTrue(dict1.containsAnyValueGtEq(4L));
  }

  @Test
  public void gtEqValue2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gtEq = dict1.findIdsOfValuesGtEq(5L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 1L }));

    Assert.assertEquals(gtEq, expected);
    Assert.assertTrue(dict1.containsAnyValueGtEq(5L));
  }

  @Test
  public void gtEqValue3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gtEq = dict1.findIdsOfValuesGtEq(0L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L }));

    Assert.assertEquals(gtEq, expected);
    Assert.assertTrue(dict1.containsAnyValueGtEq(0L));
  }

  @Test
  public void gtEqValue4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gtEq = dict1.findIdsOfValuesGtEq(1L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L }));

    Assert.assertEquals(gtEq, expected);
    Assert.assertTrue(dict1.containsAnyValueGtEq(1L));
  }

  @Test
  public void gtEqValue5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gtEq = dict1.findIdsOfValuesGtEq(6L);

    // THEN
    Set<Long> expected = new HashSet<>();

    Assert.assertEquals(gtEq, expected);
    Assert.assertFalse(dict1.containsAnyValueGtEq(6L));
  }

  @Test
  public void gtValue1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gt = dict1.findIdsOfValuesGt(4L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 1L }));

    Assert.assertEquals(gt, expected);
    Assert.assertTrue(dict1.containsAnyValueGt(4L));
  }

  @Test
  public void gtValue2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 0, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gt = dict1.findIdsOfValuesGt(5L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] {}));

    Assert.assertEquals(gt, expected);
    Assert.assertFalse(dict1.containsAnyValueGt(5L));
  }

  @Test
  public void gtValue3Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gt = dict1.findIdsOfValuesGt(0L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 0L, 1L }));

    Assert.assertEquals(gt, expected);
    Assert.assertTrue(dict1.containsAnyValueGt(0L));
  }

  @Test
  public void gtValue4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gt = dict1.findIdsOfValuesGt(1L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] { 1L }));

    Assert.assertEquals(gt, expected);
    Assert.assertTrue(dict1.containsAnyValueGt(1L));
  }

  @Test
  public void gtValue5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Set<Long> gt = dict1.findIdsOfValuesGt(6L);

    // THEN
    Set<Long> expected = new HashSet<>(Arrays.asList(new Long[] {}));

    Assert.assertEquals(gt, expected);
    Assert.assertFalse(dict1.containsAnyValueGt(6L));
  }

  @Test
  public void idGtEq1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(5L);

    // THEN
    Assert.assertEquals((long) id, 1L);
  }

  @Test
  public void idGtEq2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(6L);

    // THEN
    Assert.assertNull(id);
  }

  @Test
  public void idGtEq4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(4L);

    // THEN
    Assert.assertEquals((long) id, -(1 + 1));
  }

  @Test
  public void idGtEq5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(2L);

    // THEN
    Assert.assertEquals((long) id, -(1 + 1));
  }

  @Test
  public void idGtEq6Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(0L);

    // THEN
    Assert.assertEquals((long) id, -(0 + 1));
  }

  @Test
  public void idGtEq7Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findGtEqIdOfValue(1L);

    // THEN
    Assert.assertEquals((long) id, 0L);
  }

  @Test
  public void idLtEq1Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(5L);

    // THEN
    Assert.assertEquals((long) id, 1L);
  }

  @Test
  public void idLtEq2Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(0L);

    // THEN
    Assert.assertNull(id);
  }

  @Test
  public void idLtEq4Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(4L);

    // THEN
    Assert.assertEquals((long) id, -(0 + 1));
  }

  @Test
  public void idLtEq5Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(2L);

    // THEN
    Assert.assertEquals((long) id, -(0 + 1));
  }

  @Test
  public void idLtEq6Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(6L);

    // THEN
    Assert.assertEquals((long) id, -(1 + 1));
  }

  @Test
  public void idLtEq7Test() {
    // GIVEN
    CompressedLongArray array1 = new BitEfficientLongArray(new long[] { 1, 5 }, true);
    ArrayCompressedLongDictionary dict1 = new ArrayCompressedLongDictionary(array1);

    // WHEN
    Long id = dict1.findLtEqIdOfValue(1L);

    // THEN
    Assert.assertEquals((long) id, 0L);
  }

  @Test
  public void ltEqToConstantDictTest() {
    // GIVEN
    CompressedLongArray array = new BitEfficientLongArray(new long[] { 0, 3 }, true);
    ArrayCompressedLongDictionary compressedDict = new ArrayCompressedLongDictionary(array);

    ConstantLongDictionary constantDict = new ConstantLongDictionary(2, 0);

    // WHEN
    Map<Long, Long> ltEq = compressedDict.findLtEqIds(constantDict);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(expected, ltEq);

    // WHEN THEN
    ltEq = constantDict.findLtEqIds(compressedDict);
    expected = new HashMap<>();
    expected.put(0L, -2L);
    Assert.assertEquals(expected, ltEq);
  }

  @Test
  public void gtEqToConstantDictTest() {
    // GIVEN
    CompressedLongArray array = new BitEfficientLongArray(new long[] { 0, 3 }, true);
    ArrayCompressedLongDictionary compressedDict = new ArrayCompressedLongDictionary(array);

    ConstantLongDictionary constantDict = new ConstantLongDictionary(2, 0);

    // WHEN
    Map<Long, Long> gtEq = compressedDict.findGtEqIds(constantDict);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, -1L);
    Assert.assertEquals(expected, gtEq);

    // WHEN THEN
    gtEq = constantDict.findGtEqIds(compressedDict);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(expected, gtEq);
  }
}
