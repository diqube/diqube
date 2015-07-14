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
package org.diqube.data.str;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import org.diqube.data.str.dict.ConstantStringDictionary;
import org.diqube.data.str.dict.TrieNode;
import org.diqube.data.str.dict.TrieNode.ParentNode;
import org.diqube.data.str.dict.TrieNode.TerminalNode;
import org.diqube.data.str.dict.TrieStringDictionary;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.HashBiMap;

/**
 * TODO test dict-compare functions with a constant dict, too.
 * 
 * @author Bastian Gloeckle
 */
public class TrieStringDictionaryTest {
  @Test
  public void smallTrieEqualsTest() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("abc", terminal(0)), //
        new Pair<>("bcd", terminal(1)));

    TrieStringDictionary dict = new TrieStringDictionary(root, "abc", "bcd", 1);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "abc" }));
    Assert.assertTrue(dict.containsAnyValue(new String[] { "aaa", "bcd" }));
    Assert.assertEquals(dict.findIdOfValue("abc"), 0);
    Assert.assertEquals(dict.findIdOfValue("bcd"), 1);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "aaa", "abc", "bcd" }), new Long[] { -1L, 0L, 1L });
    Assert.assertEquals(dict.decompressValue(0), "abc");
    Assert.assertEquals(dict.decompressValue(1), "bcd");
    Assert.assertEquals(dict.decompressValues(new Long[] { 0L, 1L }), new String[] { "abc", "bcd" });
  }

  @Test
  public void smallTrieEquals2Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a", terminal(0)), //
        new Pair<>("b", terminal(1)), //
        new Pair<>("c", terminal(2)));

    TrieStringDictionary dict = new TrieStringDictionary(root, "b", "c", 2);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "a" }));
    Assert.assertTrue(dict.containsAnyValue(new String[] { "aaa", "c" }));
    Assert.assertEquals(dict.findIdOfValue("a"), 0);
    Assert.assertEquals(dict.findIdOfValue("b"), 1);
    Assert.assertEquals(dict.findIdOfValue("c"), 2);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "a", "b", "c", "zz" }), new Long[] { 0L, 1L, 2L, -1L });
    Assert.assertEquals(dict.decompressValue(0), "a");
    Assert.assertEquals(dict.decompressValue(1), "b");
    Assert.assertEquals(dict.decompressValue(2), "c");
    Assert.assertEquals(dict.decompressValues(new Long[] { 0L, 1L, 2L }), new String[] { "a", "b", "c" });
  }

  @Test
  public void oneParentTrieEqualsTest() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a",
            parent( //
                new Pair<>("bc", terminal(0)), //
                new Pair<>("cd", terminal(1)))));

    TrieStringDictionary dict = new TrieStringDictionary(root, "abc", "acd", 1);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "abc" }));
    Assert.assertTrue(dict.containsAnyValue(new String[] { "aaa", "acd" }));
    Assert.assertEquals(dict.findIdOfValue("abc"), 0);
    Assert.assertEquals(dict.findIdOfValue("acd"), 1);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "aaa", "abc", "acd" }), new Long[] { -1L, 0L, 1L });
    Assert.assertEquals(dict.decompressValue(0), "abc");
    Assert.assertEquals(dict.decompressValue(1), "acd");
    Assert.assertEquals(dict.decompressValues(new Long[] { 0L, 1L }), new String[] { "abc", "acd" });
  }

  @Test
  public void anotherOneParentTrieEqualsTest() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("ab",
            parent( //
                new Pair<>("c", terminal(0)), //
                new Pair<>("d", terminal(1)))));

    TrieStringDictionary dict = new TrieStringDictionary(root, "abc", "abd", 1);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "abc" }));
    Assert.assertTrue(dict.containsAnyValue(new String[] { "aaa", "abd" }));
    Assert.assertEquals(dict.findIdOfValue("abc"), 0);
    Assert.assertEquals(dict.findIdOfValue("abd"), 1);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "aaa", "abc", "abd" }), new Long[] { -1L, 0L, 1L });
    Assert.assertEquals(dict.decompressValue(0), "abc");
    Assert.assertEquals(dict.decompressValue(1), "abd");
    Assert.assertEquals(dict.decompressValues(new Long[] { 0L, 1L }), new String[] { "abc", "abd" });
  }

  @Test
  public void oneParentEmptyTerminalTrieEqualsTest() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("ab",
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("d", terminal(1)))));

    TrieStringDictionary dict = new TrieStringDictionary(root, "ab", "abd", 1);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "ab" }));
    Assert.assertTrue(dict.containsAnyValue(new String[] { "aaa", "abd" }));
    Assert.assertEquals(dict.findIdOfValue("ab"), 0);
    Assert.assertEquals(dict.findIdOfValue("abd"), 1);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "aaa", "ab", "abd" }), new Long[] { -1L, 0L, 1L });
    Assert.assertEquals(dict.decompressValue(0), "ab");
    Assert.assertEquals(dict.decompressValue(1), "abd");
    Assert.assertEquals(new String[] { "ab", "abd" }, dict.decompressValues(new Long[] { 0L, 1L }));
  }

  @Test
  public void emptyStringEqualsTest() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("", terminal(0)));

    TrieStringDictionary dict = new TrieStringDictionary(root, "", "", 0);

    // WHEN THEN
    Assert.assertTrue(dict.containsAnyValue(new String[] { "" }));
    Assert.assertFalse(dict.containsAnyValue(new String[] { "aaa", "abd" }));
    Assert.assertEquals(dict.findIdOfValue(""), 0);
    Assert.assertEquals(dict.findIdsOfValues(new String[] { "aaa", "" }), new Long[] { -1L, 0L });
    Assert.assertEquals(dict.decompressValue(0), "");
    Assert.assertEquals(dict.decompressValues(new Long[] { 0L }), new String[] { "" });
  }

  @Test
  public void ltEq1Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("", terminal(0)));

    TrieStringDictionary dict = new TrieStringDictionary(root, "", "", 0);

    // WHEN THEN
    Assert.assertEquals((long) dict.findLtEqIdOfValue("a"), -1L);
    Assert.assertEquals((long) dict.findLtEqIdOfValue(""), 0L);
    Assert.assertEquals(dict.findIdsOfValuesLtEq("a"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("a"));
    Assert.assertEquals(dict.findIdsOfValuesLtEq(""), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLtEq(""));

    // test LT methods, too
    Assert.assertEquals(dict.findIdsOfValuesLt("a"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLt("a"));
    Assert.assertEquals(dict.findIdsOfValuesLt(""), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueLt(""));
  }

  @Test
  public void ltEq2Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict = new TrieStringDictionary(root, "a", "abc", 1);

    // WHEN THEN
    Assert.assertEquals((long) dict.findLtEqIdOfValue("a"), 0L);
    Assert.assertEquals((long) dict.findLtEqIdOfValue("aa"), -(0L + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue("abc"), 1L);
    Assert.assertEquals((long) dict.findLtEqIdOfValue("aba"), -(0L + 1));
    Assert.assertEquals((long) dict.findLtEqIdOfValue("b"), -(1L + 1));
    Assert.assertNull(dict.findLtEqIdOfValue(""));
    Assert.assertEquals(dict.findIdsOfValuesLtEq(""), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueLtEq(""));
    Assert.assertEquals(dict.findIdsOfValuesLtEq("a"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("a"));
    Assert.assertEquals(dict.findIdsOfValuesLtEq("aa"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("aa"));
    Assert.assertEquals(dict.findIdsOfValuesLtEq("aba"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("aba"));
    Assert.assertEquals(dict.findIdsOfValuesLtEq("abc"), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("abc"));
    Assert.assertEquals(dict.findIdsOfValuesLtEq("b"), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueLtEq("b"));

    // test LT methods, too
    Assert.assertEquals(dict.findIdsOfValuesLt(""), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueLt(""));
    Assert.assertEquals(dict.findIdsOfValuesLt("a"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueLt("a"));
    Assert.assertEquals(dict.findIdsOfValuesLt("aa"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLt("aa"));
    Assert.assertEquals(dict.findIdsOfValuesLt("aba"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLt("aba"));
    Assert.assertEquals(dict.findIdsOfValuesLt("abc"), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueLt("abc"));
    Assert.assertEquals(dict.findIdsOfValuesLt("b"), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueLt("b"));
  }

  @Test
  public void gtEq1Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("", terminal(0)));

    TrieStringDictionary dict = new TrieStringDictionary(root, "", "", 0);

    // WHEN THEN
    Assert.assertNull(dict.findGtEqIdOfValue("a"));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(""), 0L);
    Assert.assertEquals(dict.findIdsOfValuesGtEq("a"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGtEq("a"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq(""), new HashSet<>(Arrays.asList(new Long[] { 0L })));
    Assert.assertTrue(dict.containsAnyValueGtEq(""));

    // test GT methods, too
    Assert.assertEquals(dict.findIdsOfValuesGt("a"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGt("a"));
    Assert.assertEquals(dict.findIdsOfValuesGt(""), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGt(""));
  }

  @Test
  public void gtEq2Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict = new TrieStringDictionary(root, "a", "abc", 1);

    // WHEN THEN
    Assert.assertEquals((long) dict.findGtEqIdOfValue("a"), 0L);
    Assert.assertEquals((long) dict.findGtEqIdOfValue("aa"), -(1L + 1));
    Assert.assertEquals((long) dict.findGtEqIdOfValue("abc"), 1L);
    Assert.assertEquals((long) dict.findGtEqIdOfValue("aba"), -(1L + 1));
    Assert.assertEquals((long) dict.findGtEqIdOfValue(""), -(0L + 1));
    Assert.assertNull(dict.findGtEqIdOfValue("b"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq(""), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueGtEq(""));
    Assert.assertEquals(dict.findIdsOfValuesGtEq("a"), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueGtEq("a"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq("aa"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGtEq("aa"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq("aba"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGtEq("aba"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq("abc"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGtEq("abc"));
    Assert.assertEquals(dict.findIdsOfValuesGtEq("b"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGtEq("b"));

    // test GT methods, too
    Assert.assertEquals(dict.findIdsOfValuesGt(""), new HashSet<>(Arrays.asList(new Long[] { 0L, 1L })));
    Assert.assertTrue(dict.containsAnyValueGt(""));
    Assert.assertEquals(dict.findIdsOfValuesGt("a"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGt("a"));
    Assert.assertEquals(dict.findIdsOfValuesGt("aa"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGt("aa"));
    Assert.assertEquals(dict.findIdsOfValuesGt("aba"), new HashSet<>(Arrays.asList(new Long[] { 1L })));
    Assert.assertTrue(dict.containsAnyValueGt("aba"));
    Assert.assertEquals(dict.findIdsOfValuesGt("abc"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGt("abc"));
    Assert.assertEquals(dict.findIdsOfValuesGt("b"), new HashSet<>(Arrays.asList(new Long[] {})));
    Assert.assertFalse(dict.containsAnyValueGt("b"));
  }

  @Test
  public void dictCompareEqual1Test() {
    // GIVEN
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "a", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "a", "abc", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual2Test() {
    // GIVEN

    // contains strings:
    // a
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "a", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aa", "abc", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual3Test() {
    // GIVEN

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent( //
                        new Pair<>("", terminal(0)))), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aa", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aa", "abc", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual4Test() {
    // GIVEN

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aaa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent( //
                        new Pair<>("a", terminal(0)))), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aa", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aaa", "abc", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual5Test() {
    // GIVEN

    // contains strings:
    // aa
    // aabc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("aa", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent( //
                        new Pair<>("", terminal(0)))), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aa", "aabc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aa", "abc", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual6Test() {
    // GIVEN

    // contains strings:
    // aa
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("aa", //
            parent( //
                new Pair<>("", terminal(0)))));

    // contains strings:
    // a
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", terminal(0)));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aa", "aa", 0);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "a", "a", 0);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Assert.assertEquals(eqIds, new HashMap<>());

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual7Test() {
    // GIVEN

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("ab", //
            parent( //
                new Pair<>("c", terminal(0)), //
                new Pair<>("d", terminal(1)))));

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abc", "abd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abc", "abd", 1);

    // WHEN
    // This test will lead to a "prefix" being carried on: "ab" root node of dict1 will match first to "a" root node of
    // dict2, but the remaining "b" has to be carried over to the next node. This means that the intermediary "b" node
    // in dict2 has to be matched to the two dict1-strings (including prefix) "bc" "bd".
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual8Test() {
    // GIVEN

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("bc", terminal(0)), //
                new Pair<>("bd", terminal(1)))));

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abc", "abd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abc", "abd", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual9Test() {
    // GIVEN

    // contains strings:
    // abcx
    // abdy
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("ab", //
            parent( //
                new Pair<>("c", //
                    parent(new Pair<>("x", terminal(0)))), //
                new Pair<>("d", //
                    parent(new Pair<>("y", terminal(1)))))));

    // contains strings:
    // abcx
    // abdy
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent(//
                        new Pair<>("c", //
                            parent(new Pair<>("x", terminal(0)))), //
                        new Pair<>("d", //
                            parent(new Pair<>("y", terminal(1)))))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abcx", "abdy", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abcx", "abdy", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual10Test() {
    // GIVEN

    // contains strings:
    // abcx
    // abdy
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("bc", //
                    parent(new Pair<>("x", terminal(0)))), //
                new Pair<>("bd", //
                    parent(new Pair<>("y", terminal(1)))))));

    // contains strings:
    // abcx
    // abdy
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent(//
                        new Pair<>("c", //
                            parent(new Pair<>("x", terminal(0)))), //
                        new Pair<>("d", //
                            parent(new Pair<>("y", terminal(1)))))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abcx", "abdy", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abcx", "abdy", 1);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 0L);
    expected.put(1L, 1L);
    Assert.assertEquals(eqIds, expected);

    Assert.assertEquals(dict2.findEqualIds(dict1), eqIds);
  }

  @Test
  public void dictCompareEqual11Test() {
    // GIVEN

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    // contains strings:
    // abac
    // abad
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ba", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))), //
                new Pair<>("bc", terminal(2)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abc", "abd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abac", "abc", 2);

    // WHEN
    Map<Long, Long> eqIds = dict1.findEqualIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 2L);
    Assert.assertEquals(eqIds, expected);

    expected = HashBiMap.create(expected).inverse();
    Assert.assertEquals(dict2.findEqualIds(dict1), expected);
  }

  @Test
  public void dictCompareGtEq1Test() {
    // GIVEN

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    // contains strings:
    // abac
    // abad
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ba", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))), //
                new Pair<>("bc", terminal(2)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abc", "abd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abac", "abc", 2);

    // WHEN
    Map<Long, Long> grEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 2L);
    expected.put(1L, -3L);
    Assert.assertEquals(grEqIds, expected);

    // WHEN
    grEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(2L, 0L);
    Assert.assertEquals(grEqIds, expected);
  }

  @Test
  public void dictCompareGtEq2Test() {
    // GIVEN

    // contains strings:
    // xbc
    // xbd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    // contains strings:
    // abac
    // abad
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ba", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))), //
                new Pair<>("bc", terminal(2)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xbc", "xbd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abac", "abc", 2);

    // WHEN
    Map<Long, Long> grEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -3L);
    expected.put(1L, -3L);
    Assert.assertEquals(grEqIds, expected);

    // WHEN
    grEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    Assert.assertEquals(grEqIds, expected);
  }

  @Test
  public void dictCompareGtEq3Test() {
    // GIVEN

    // contains strings:
    // xaby
    // xacy
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("ab", //
                    parent( //
                        new Pair<>("y", terminal(0)))), //
                new Pair<>("ac", //
                    parent( //
                        new Pair<>("y", terminal(1)))))));

    // contains strings:
    // xa
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("a", terminal(0)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xaby", "xacy", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "xa", "xa", 0);

    // WHEN
    Map<Long, Long> grEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, -1L);
    Assert.assertEquals(grEqIds, expected);

    // WHEN
    grEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    Assert.assertEquals(grEqIds, expected);
  }

  @Test
  public void dictCompareGtEq4Test() {
    // GIVEN

    // contains strings:
    // a
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "a", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aa", "abc", 1);

    // WHEN
    Map<Long, Long> gtEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 1L);
    Assert.assertEquals(gtEqIds, expected);

    // WHEN
    gtEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, 1L);
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void dictCompareGtEq5Test() {
    // GIVEN

    // contains strings:
    // xab
    // xac
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("ab", terminal(0)), //
                new Pair<>("ac", terminal(1)))));

    // contains strings:
    // xa
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("a", terminal(0)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xab", "xac", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "xa", "xa", 0);

    // WHEN
    Map<Long, Long> gtEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, -1L);
    Assert.assertEquals(gtEqIds, expected);

    // WHEN
    gtEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void dictCompareGtEq6Test() {
    // GIVEN

    // contains strings:
    // aab
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ab", terminal(0)))));

    // contains strings:
    // aaa
    // aac
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent(new Pair<>("a", terminal(0)), //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aab", "aab", 0);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aaa", "aac", 1);

    // WHEN
    Map<Long, Long> gtEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(gtEqIds, expected);

    // WHEN
    gtEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(1L, -1L);
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void dictCompareGtEq7Test() {
    // GIVEN

    // contains strings:
    // aab (0)
    // bab (1)
    // bad (2)
    // bae (3)
    // baf (4)
    // bbf (5)
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("aab", terminal(0)), //
        new Pair<>("b",
            parent( //
                new Pair<>("a",
                    parent( //
                        new Pair<>("b", terminal(1)), //
                        new Pair<>("d", terminal(2)), //
                        new Pair<>("e", terminal(3)), //
                        new Pair<>("f", terminal(4)) //
    )), //
                new Pair<>("bf", terminal(5)))));

    // contains strings:
    // baa (0)
    // bab (1)
    // bac (2)
    // bad (3)
    // bba (4)
    // bbb (5)
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("b",
            parent( //
                new Pair<>("a",
                    parent( //
                        new Pair<>("a", terminal(0)), //
                        new Pair<>("b", terminal(1)), //
                        new Pair<>("c", terminal(2)), //
                        new Pair<>("d", terminal(3)) //
    )), //
                new Pair<>("b",
                    parent( //
                        new Pair<>("a", terminal(4)), //
                        new Pair<>("b", terminal(5)) //
    )))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aab", "bbf", 5);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "baa", "bbb", 5);

    // WHEN
    Map<Long, Long> gtEqIds = dict1.findGtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, 1L);
    expected.put(2L, 3L);
    expected.put(3L, -4L);
    expected.put(4L, -4L);
    expected.put(5L, -6L);
    Assert.assertEquals(gtEqIds, expected);

    // WHEN
    gtEqIds = dict2.findGtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, 1L);
    expected.put(2L, -2L);
    expected.put(3L, 2L);
    expected.put(4L, -5L);
    expected.put(5L, -5L);
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void dictCompareLtEq1Test() {
    // GIVEN

    // contains strings:
    // abc
    // abd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    // contains strings:
    // abac
    // abad
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ba", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))), //
                new Pair<>("bc", terminal(2)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "abc", "abd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abac", "abc", 2);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, 2L);
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, -1L);
    expected.put(2L, 0L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq2Test() {
    // GIVEN

    // contains strings:
    // xbc
    // xbd
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))))));

    // contains strings:
    // abac
    // abad
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ba", //
                    parent(//
                        new Pair<>("c", terminal(0)), //
                        new Pair<>("d", terminal(1)))), //
                new Pair<>("bc", terminal(2)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xbc", "xbd", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "abac", "abc", 2);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, -1L);
    expected.put(2L, -1L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq3Test() {
    // GIVEN

    // contains strings:
    // xaby
    // xacy
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("ab", //
                    parent( //
                        new Pair<>("y", terminal(0)))), //
                new Pair<>("ac", //
                    parent( //
                        new Pair<>("y", terminal(1)))))));

    // contains strings:
    // xa
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("a", terminal(0)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xaby", "xacy", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "xa", "xa", 0);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq4Test() {
    // GIVEN

    // contains strings:
    // a
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    // contains strings:
    // aa
    // abc
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", terminal(0)), //
                new Pair<>("b", //
                    parent( //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "a", "abc", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aa", "abc", 1);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, 1L);
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -2L);
    expected.put(1L, 1L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq5Test() {
    // GIVEN

    // contains strings:
    // xab
    // xac
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("ab", terminal(0)), //
                new Pair<>("ac", terminal(1)))));

    // contains strings:
    // xa
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("x", //
            parent( //
                new Pair<>("a", terminal(0)))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "xab", "xac", 1);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "xa", "xa", 0);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq6Test() {
    // GIVEN

    // contains strings:
    // aab
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("ab", terminal(0)))));

    // contains strings:
    // aaa
    // aac
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent(new Pair<>("a", terminal(0)), //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aab", "aab", 0);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "aaa", "aac", 1);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -2L);
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void dictCompareLtEq7Test() {
    // GIVEN

    // contains strings:
    // aab (0)
    // bab (1)
    // bad (2)
    // bae (3)
    // baf (4)
    // bbf (5)
    @SuppressWarnings("unchecked")
    ParentNode root1 = parent( //
        new Pair<>("aab", terminal(0)), //
        new Pair<>("b",
            parent( //
                new Pair<>("a",
                    parent( //
                        new Pair<>("b", terminal(1)), //
                        new Pair<>("d", terminal(2)), //
                        new Pair<>("e", terminal(3)), //
                        new Pair<>("f", terminal(4)) //
    )), //
                new Pair<>("bf", terminal(5)))));

    // contains strings:
    // baa (0)
    // bab (1)
    // bac (2)
    // bad (3)
    // bba (4)
    // bbb (5)
    @SuppressWarnings("unchecked")
    ParentNode root2 = parent( //
        new Pair<>("b",
            parent( //
                new Pair<>("a",
                    parent( //
                        new Pair<>("a", terminal(0)), //
                        new Pair<>("b", terminal(1)), //
                        new Pair<>("c", terminal(2)), //
                        new Pair<>("d", terminal(3)) //
    )), //
                new Pair<>("b",
                    parent( //
                        new Pair<>("a", terminal(4)), //
                        new Pair<>("b", terminal(5)) //
    )))));

    TrieStringDictionary dict1 = new TrieStringDictionary(root1, "aab", "bbf", 5);
    TrieStringDictionary dict2 = new TrieStringDictionary(root2, "baa", "bbb", 5);

    // WHEN
    Map<Long, Long> ltEqIds = dict1.findLtEqIds(dict2);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    expected.put(1L, 1L);
    expected.put(2L, 3L);
    expected.put(3L, -5L);
    expected.put(4L, -5L);
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = dict2.findLtEqIds(dict1);
    expected = new HashMap<>();
    expected.put(0L, -2L);
    expected.put(1L, 1L);
    expected.put(2L, -3L);
    expected.put(3L, 2L);
    expected.put(4L, -6L);
    expected.put(5L, -6L);
    Assert.assertEquals(ltEqIds, expected);
  }

  @Test
  public void gtEqCompareToConstantDict() {
    // GIVEN

    // contains strings:
    // aaa
    // aac
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent(new Pair<>("a", terminal(0)), //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary trieDict = new TrieStringDictionary(root, "aaa", "aac", 1);

    ConstantStringDictionary constantDict = new ConstantStringDictionary("aab", 0L);

    // WHEN
    Map<Long, Long> gtEqIds = trieDict.findGtEqIds(constantDict);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(1L, -1L);
    Assert.assertEquals(gtEqIds, expected);

    // WHEN
    gtEqIds = constantDict.findGtEqIds(trieDict);
    expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(gtEqIds, expected);
  }

  @Test
  public void ltEqCompareToConstantDict() {
    // GIVEN

    // contains strings:
    // aaa
    // aac
    @SuppressWarnings("unchecked")
    ParentNode root = parent( //
        new Pair<>("a", //
            parent( //
                new Pair<>("a", //
                    parent(new Pair<>("a", terminal(0)), //
                        new Pair<>("c", terminal(1)))))));

    TrieStringDictionary trieDict = new TrieStringDictionary(root, "aaa", "aac", 1);

    ConstantStringDictionary constantDict = new ConstantStringDictionary("aab", 0L);

    // WHEN
    Map<Long, Long> ltEqIds = trieDict.findLtEqIds(constantDict);

    // THEN
    Map<Long, Long> expected = new HashMap<>();
    expected.put(0L, -1L);
    Assert.assertEquals(ltEqIds, expected);

    // WHEN
    ltEqIds = constantDict.findLtEqIds(trieDict);
    expected = new HashMap<>();
    expected.put(0L, -2L);
    Assert.assertEquals(ltEqIds, expected);
  }

  private ParentNode parent(Pair<String, TrieNode>... children) {
    char[][] childChars =
        Stream.of(children).map(p -> p.getLeft()).map(s -> s.toCharArray()).toArray(l -> new char[l][]);
    TrieNode[] childNodes = Stream.of(children).map(p -> p.getRight()).toArray(l -> new TrieNode[l]);
    long minId, maxId;
    if (childNodes[0] instanceof TerminalNode)
      minId = ((TerminalNode) childNodes[0]).getTerminalId();
    else
      minId = ((ParentNode) childNodes[0]).getMinId();

    if (childNodes[childNodes.length - 1] instanceof TerminalNode)
      maxId = ((TerminalNode) childNodes[childNodes.length - 1]).getTerminalId();
    else
      maxId = ((ParentNode) childNodes[childNodes.length - 1]).getMaxId();

    return new ParentNode(childChars, childNodes, minId, maxId);
  }

  private TerminalNode terminal(long terminalId) {
    return new TerminalNode(terminalId);
  }
}
