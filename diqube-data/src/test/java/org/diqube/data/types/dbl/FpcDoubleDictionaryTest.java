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
package org.diqube.data.types.dbl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.types.dbl.dict.FpcDoubleDictionary;
import org.diqube.data.types.dbl.dict.FpcPage;
import org.diqube.data.types.dbl.dict.FpcPage.State;
import org.diqube.util.DoubleUtil;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

/**
 *
 * TODO test dict-compare functions with a constant dict, too.
 * 
 * @author Bastian Gloeckle
 */
public class FpcDoubleDictionaryTest {
  @Test
  public void simpleTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);
    page1.compress(new double[] { 0.5, 1.5, 2.5, 3.5, 4.5 });

    FpcDoubleDictionary dict = createDict(0.5, 4.5, page1);

    // WHEN/THEN
    Assert.assertTrue(DoubleUtil.equals(0.5, dict.decompressValue(0)));
    Assert.assertTrue(DoubleUtil.equals(1.5, dict.decompressValue(1)));
    Assert.assertTrue(DoubleUtil.equals(2.5, dict.decompressValue(2)));
    Assert.assertTrue(DoubleUtil.equals(3.5, dict.decompressValue(3)));
    Assert.assertTrue(DoubleUtil.equals(4.5, dict.decompressValue(4)));
    Assert.assertEquals(dict.findIdOfValue(0.5), 0);
    Assert.assertEquals(dict.findIdOfValue(1.5), 1);
    Assert.assertEquals(dict.findIdOfValue(2.5), 2);
    Assert.assertEquals(dict.findIdOfValue(3.5), 3);
    Assert.assertEquals(dict.findIdOfValue(4.5), 4);
    Assert.assertEquals(dict.findIdsOfValues(new Double[] { 0.5, 4.5, 5. }), new Long[] { 0L, 4L, -1L });
    Assert.assertTrue(dict.containsAnyValue(new Double[] { 0., 3., 0.5 }));

    Assert.assertEquals((long) dict.findLtEqIdOfValue(5.), -(4 + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(4.5), 4);
    Assert.assertEquals((long) dict.findLtEqIdOfValue(4.), -(3 + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(1.), -(0 + 1));
    Assert.assertNull(dict.findLtEqIdOfValue(0.));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(0.), -(0 + 1));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(0.5), 0);
    Assert.assertEquals((long) dict.findGtEqIdOfValue(1.), -(1 + 1));
    Assert.assertNull(dict.findGtEqIdOfValue(5.));

    Assert.assertEquals(dict.findIdsOfValuesGt(4.0), new HashSet<>(Arrays.asList(new Long[] { 4L })));
    Assert.assertEquals(dict.findIdsOfValuesGt(4.5), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueGt(4.0));
    Assert.assertFalse(dict.containsAnyValueGt(4.5));

    Assert.assertEquals(dict.findIdsOfValuesGtEq(4.5), new HashSet<>(Arrays.asList(new Long[] { 4L })));
    Assert.assertEquals(dict.findIdsOfValuesGtEq(5.0), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueGtEq(4.5));
    Assert.assertFalse(dict.containsAnyValueGtEq(5.));

    Assert.assertEquals(dict.findIdsOfValuesLt(1.0), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertEquals(dict.findIdsOfValuesLt(0.5), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueLt(1.0));
    Assert.assertFalse(dict.containsAnyValueLt(0.5));

    Assert.assertEquals(dict.findIdsOfValuesLtEq(0.5), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertEquals(dict.findIdsOfValuesLtEq(0.4), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueLtEq(0.5));
    Assert.assertFalse(dict.containsAnyValueLtEq(0.));
  }

  @Test
  public void twoPageTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);
    State statePage1 = page1.compress(new double[] { 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page2 = new FpcPage(5L, statePage1);
    page2.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });

    FpcDoubleDictionary dict = createDict(0.5, 9.5, page1, page2);

    // WHEN/THEN
    Assert.assertTrue(DoubleUtil.equals(0.5, dict.decompressValue(0)));
    Assert.assertTrue(DoubleUtil.equals(1.5, dict.decompressValue(1)));
    Assert.assertTrue(DoubleUtil.equals(2.5, dict.decompressValue(2)));
    Assert.assertTrue(DoubleUtil.equals(3.5, dict.decompressValue(3)));
    Assert.assertTrue(DoubleUtil.equals(4.5, dict.decompressValue(4)));
    Assert.assertTrue(DoubleUtil.equals(5.5, dict.decompressValue(5)));
    Assert.assertTrue(DoubleUtil.equals(6.5, dict.decompressValue(6)));
    Assert.assertTrue(DoubleUtil.equals(7.5, dict.decompressValue(7)));
    Assert.assertTrue(DoubleUtil.equals(8.5, dict.decompressValue(8)));
    Assert.assertTrue(DoubleUtil.equals(9.5, dict.decompressValue(9)));
    Assert.assertEquals(dict.findIdOfValue(0.5), 0);
    Assert.assertEquals(dict.findIdOfValue(1.5), 1);
    Assert.assertEquals(dict.findIdOfValue(2.5), 2);
    Assert.assertEquals(dict.findIdOfValue(3.5), 3);
    Assert.assertEquals(dict.findIdOfValue(4.5), 4);
    Assert.assertEquals(dict.findIdOfValue(5.5), 5);
    Assert.assertEquals(dict.findIdOfValue(6.5), 6);
    Assert.assertEquals(dict.findIdOfValue(7.5), 7);
    Assert.assertEquals(dict.findIdOfValue(8.5), 8);
    Assert.assertEquals(dict.findIdOfValue(9.5), 9);
    Assert.assertEquals(dict.findIdsOfValues(new Double[] { 5.5, 4.4, 5. }), new Long[] { 5L, -1L, -1L });
    Assert.assertTrue(dict.containsAnyValue(new Double[] { 0., 3., 9.5 }));

    Assert.assertEquals((long) dict.findLtEqIdOfValue(5.), -(4 + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(4.5), 4);
    Assert.assertEquals((long) dict.findLtEqIdOfValue(4.), -(3 + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(1.), -(0 + 1));
    Assert.assertNull(dict.findLtEqIdOfValue(0.));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(0.), -(0 + 1));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(0.5), 0);
    Assert.assertEquals((long) dict.findGtEqIdOfValue(1.), -(1 + 1));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(5.), -(5 + 1));
    Assert.assertNull(dict.findGtEqIdOfValue(9.6));

    Assert.assertEquals(dict.findIdsOfValuesGt(4.0),
        new HashSet<>(Arrays.asList(new Long[] { 4L, 5L, 6L, 7L, 8L, 9L })));
    Assert.assertEquals(dict.findIdsOfValuesGt(4.5), new HashSet<>(Arrays.asList(new Long[] { 5L, 6L, 7L, 8L, 9L })));
    Assert.assertEquals(dict.findIdsOfValuesGt(9.5), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueGt(4.5));
    Assert.assertFalse(dict.containsAnyValueGt(9.5));

    Assert.assertEquals(dict.findIdsOfValuesGtEq(4.5),
        new HashSet<>(Arrays.asList(new Long[] { 4L, 5L, 6L, 7L, 8L, 9L })));
    Assert.assertEquals(dict.findIdsOfValuesGtEq(4.6), new HashSet<>(Arrays.asList(new Long[] { 5L, 6L, 7L, 8L, 9L })));
    Assert.assertTrue(dict.containsAnyValueGtEq(9.5));
    Assert.assertFalse(dict.containsAnyValueGtEq(9.6));

    Assert.assertEquals(dict.findIdsOfValuesLt(5.5), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L })));
    Assert.assertEquals(dict.findIdsOfValuesLt(1.0), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertEquals(dict.findIdsOfValuesLt(0.5), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueLt(5.5));
    Assert.assertTrue(dict.containsAnyValueLt(1.0));
    Assert.assertFalse(dict.containsAnyValueLt(0.5));

    Assert.assertEquals(dict.findIdsOfValuesLtEq(5.4), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L, 2L, 3L, 4L })));
    Assert.assertEquals(dict.findIdsOfValuesLtEq(0.5), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertEquals(dict.findIdsOfValuesLtEq(0.4), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertTrue(dict.containsAnyValueLtEq(0.5));
    Assert.assertTrue(dict.containsAnyValueLtEq(5.4));
    Assert.assertFalse(dict.containsAnyValueLtEq(0.));
  }

  @Test
  public void largeTwoPageTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);
    // get somewhat "randomly" (but reconstructably) distributed numbers -> use Fibonacci numbers divided by 10
    double[] fib1 = new double[500];
    fib1[0] = 0.1;
    fib1[1] = 0.1;
    for (int i = 2; i < fib1.length; i++)
      fib1[i] = fib1[i - 1] + fib1[i - 2];

    double[] fib2 = new double[500];
    fib2[0] = fib1[fib1.length - 2] + fib1[fib1.length - 1];
    fib2[1] = fib1[fib1.length - 1] + fib2[0];
    for (int i = 2; i < fib2.length; i++)
      fib2[i] = fib2[i - 1] + fib2[i - 2];

    State statePage1 = page1.compress(fib1);
    FpcPage page2 = new FpcPage(fib1.length, statePage1);
    page2.compress(fib2);

    FpcDoubleDictionary dict = createDict(fib1[0], fib2[fib2.length - 1], page1, page2);

    // WHEN/THEN
    Assert.assertTrue(DoubleUtil.equals(fib1[fib1.length - 1], dict.decompressValue(fib1.length - 1)));
    Assert.assertTrue(DoubleUtil.equals(fib2[0], dict.decompressValue(fib1.length)));
    Assert.assertEquals(dict.findIdOfValue(fib2[0]), fib1.length);
    Assert.assertEquals(dict.findIdOfValue(fib2[fib2.length - 1]), fib1.length + fib2.length - 1);
    double beforeLast = fib2[fib2.length - 2] + ((fib2[fib2.length - 1] - fib2[fib2.length - 2]) / 2);
    Assert.assertEquals((long) dict.findGtEqIdOfValue(beforeLast), -(fib1.length + fib2.length - 1 + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(beforeLast), -(fib1.length + fib2.length - 2 + 1));

    double middle = fib1[fib1.length - 1] + ((fib2[0] - fib1[fib1.length - 1]) / 2);
    Assert.assertEquals((long) dict.findGtEqIdOfValue(middle), -(fib1.length + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue(middle), -(fib1.length - 1 + 1));
  }

  @Test
  public void compareEqualIdsTest1() {
    // GIVEN
    // 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5
    FpcPage page11 = new FpcPage(0L);
    State statePage11 = page11.compress(new double[] { 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page12 = new FpcPage(5L, statePage11);
    page12.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });
    FpcDoubleDictionary dict1 = createDict(0.5, 9.5, page11, page12);

    // 1., 1.4, 1.5, 4., 4.4, 4.8, 6.5, 100.
    FpcPage page21 = new FpcPage(0L);
    State statePage21 = page21.compress(new double[] { 1., 1.4, 1.5, 4. });
    FpcPage page22 = new FpcPage(4L, statePage21);
    page22.compress(new double[] { 4.4, 4.8, 6.5, 100. });
    FpcDoubleDictionary dict2 = createDict(1., 100., page21, page22);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 2L);
    expected.put(6L, 6L);

    Assert.assertEquals(eqIds, expected);

    expected = HashBiMap.create(expected).inverse();
    eqIds = dict2.findEqualIds(dict1);
    Assert.assertEquals(eqIds, expected);
  }

  @Test
  public void compareEqualIdsTest2() {
    // GIVEN
    // 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5
    FpcPage page11 = new FpcPage(0L);
    State statePage11 = page11.compress(new double[] { 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page12 = new FpcPage(5L, statePage11);
    page12.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });
    FpcDoubleDictionary dict1 = createDict(0.5, 9.5, page11, page12);

    // .5, 1.4, 1.5, 4., 4.4, 4.8, 6.5, 9.5
    FpcPage page21 = new FpcPage(0L);
    State statePage21 = page21.compress(new double[] { .5, 1.4, 1.5, 4. });
    FpcPage page22 = new FpcPage(4L, statePage21);
    page22.compress(new double[] { 4.4, 4.8, 6.5, 9.5 });
    FpcDoubleDictionary dict2 = createDict(.5, 9.5, page21, page22);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 2L);
    expected.put(6L, 6L);
    expected.put(9L, 7L);

    Assert.assertEquals(eqIds, expected);

    expected = HashBiMap.create(expected).inverse();
    eqIds = dict2.findEqualIds(dict1);
    Assert.assertEquals(eqIds, expected);
  }

  @Test
  public void compareLtEqIdsTest1() {
    // GIVEN
    // 0.4, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5
    FpcPage page11 = new FpcPage(0L);
    State statePage11 = page11.compress(new double[] { 0.4, 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page12 = new FpcPage(6L, statePage11);
    page12.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });
    FpcDoubleDictionary dict1 = createDict(0.5, 9.5, page11, page12);

    // .5, 1.4, 1.5, 4., 4.4, 4.8, 6.5, 9.5, 10.
    FpcPage page21 = new FpcPage(0L);
    State statePage21 = page21.compress(new double[] { .5, 1.4, 1.5, 4. });
    FpcPage page22 = new FpcPage(4L, statePage21);
    page22.compress(new double[] { 4.4, 4.8, 6.5, 9.5, 10. });
    FpcDoubleDictionary dict2 = createDict(.5, 9.5, page21, page22);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -(0 + 1L));
    expected.put(1L, 0L);
    expected.put(2L, 2L);
    expected.put(3L, -(3 + 1L));
    expected.put(4L, -(3 + 1L));
    expected.put(5L, -(5 + 1L));
    expected.put(6L, -(6 + 1L));
    expected.put(7L, 6L);
    expected.put(8L, -(7 + 1L));
    expected.put(9L, -(7 + 1L));
    expected.put(10L, 7L);

    Assert.assertEquals(ltEqIds, expected);

    expected = new HashMap<>();
    expected.put(0L, 1L);
    expected.put(1L, -(2 + 1L));
    expected.put(2L, 2L);
    expected.put(3L, -(5 + 1L));
    expected.put(4L, -(5 + 1L));
    expected.put(5L, -(6 + 1L));
    expected.put(6L, 7L);
    expected.put(7L, 10L);
    ltEqIds = dict2.findLtEqIds(dict1);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void compareGtEqIdsTest1() {
    // GIVEN
    // 0.4, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5
    FpcPage page11 = new FpcPage(0L);
    State statePage11 = page11.compress(new double[] { 0.4, 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page12 = new FpcPage(6L, statePage11);
    page12.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });
    FpcDoubleDictionary dict1 = createDict(0.5, 9.5, page11, page12);

    // .5, 1.4, 1.5, 4., 4.4, 4.8, 6.5, 9.5, 10.
    FpcPage page21 = new FpcPage(0L);
    State statePage21 = page21.compress(new double[] { .5, 1.4, 1.5, 4. });
    FpcPage page22 = new FpcPage(4L, statePage21);
    page22.compress(new double[] { 4.4, 4.8, 6.5, 9.5, 10. });
    FpcDoubleDictionary dict2 = createDict(.5, 9.5, page21, page22);

    // WHEN
    Map<Long, Long> gtEqIds = dict1.findGtEqIds(dict2);

    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 0L);
    expected.put(2L, 2L);
    expected.put(3L, -(2 + 1L));
    expected.put(4L, -(2 + 1L));
    expected.put(5L, -(4 + 1L));
    expected.put(6L, -(5 + 1L));
    expected.put(7L, 6L);
    expected.put(8L, -(6 + 1L));
    expected.put(9L, -(6 + 1L));
    expected.put(10L, 7L);

    Assert.assertEquals(gtEqIds, expected);

    expected = new HashMap<>();
    expected.put(0L, 1L);
    expected.put(1L, -(1 + 1L));
    expected.put(2L, 2L);
    expected.put(3L, -(4 + 1L));
    expected.put(4L, -(4 + 1L));
    expected.put(5L, -(5 + 1L));
    expected.put(6L, 7L);
    expected.put(7L, 10L);
    expected.put(8L, -(10 + 1L));
    gtEqIds = dict2.findGtEqIds(dict1);
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void twoPageIteratorTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);
    State statePage1 = page1.compress(new double[] { 0.5, 1.5, 2.5, 3.5, 4.5 });
    FpcPage page2 = new FpcPage(5L, statePage1);
    page2.compress(new double[] { 5.5, 6.5, 7.5, 8.5, 9.5 });

    FpcDoubleDictionary dict = createDict(0.5, 9.5, page1, page2);

    // THEN
    List<Double> expectedValues = new ArrayList<>(Arrays.asList(0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5));

    List<Pair<Long, Double>> expected = new ArrayList<>();
    for (long i = 0; i < expectedValues.size(); i++)
      expected.add(new Pair<>(i, expectedValues.get((int) i)));

    Assert.assertEquals(Lists.newArrayList(dict.iterator()), expected,
        "Expected that iterator returns correct elements.");
  }

  private FpcDoubleDictionary createDict(double lowestValue, double highestValue, FpcPage... pages) {
    NavigableMap<Long, FpcPage> pagesMap = new TreeMap<>();
    for (FpcPage page : pages)
      pagesMap.put(page.getFirstId(), page);

    return new FpcDoubleDictionary(pagesMap, lowestValue, highestValue);
  }
}
