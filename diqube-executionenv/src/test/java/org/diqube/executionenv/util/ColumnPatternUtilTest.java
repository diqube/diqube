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
package org.diqube.executionenv.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.querystats.QueryableLongColumnShard;
import org.diqube.executionenv.util.ColumnPatternUtil.ColumnPatternContainer;
import org.diqube.executionenv.util.ColumnPatternUtil.PatternException;
import org.diqube.util.Triple;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link ColumnPatternUtil}.
 *
 * @author Bastian Gloeckle
 */
public class ColumnPatternUtilTest {
  private ColumnPatternUtil colPatternUtil;
  private ExecutionEnvironment env;

  @BeforeMethod
  public void before() {
    colPatternUtil = new ColumnPatternUtil();
    colPatternUtil.setRepeatedColNames(new RepeatedColumnNameGenerator());
    env = Mockito.mock(ExecutionEnvironment.class);
  }

  @Test
  public void singlePatternSimple() {
    // GIVEN
    simulateLengths(new Triple<>("a.b", 5L, 2L));

    // WHEN
    ColumnPatternContainer patterns = colPatternUtil.findColNamesForColNamePattern(env, "a.b[*].c");
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList(Arrays.asList("a.b[0].c"), Arrays.asList("a.b[1].c")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void singlePatternSimple2() {
    // GIVEN
    simulateLengths(new Triple<>("a.b", 3L, 2L), //
        new Triple<>("a.b[0].c", 7L, 1L), //
        new Triple<>("a.b[1].c", 3L, 3L), //
        new Triple<>("a.b[2].c", 1L, 0L));

    // WHEN
    ColumnPatternContainer patterns = colPatternUtil.findColNamesForColNamePattern(env, "a.b[*].c[*]");
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a.b[0].c[0]"), //
        Arrays.asList("a.b[1].c[0]"), //
        Arrays.asList("a.b[1].c[1]"), //
        Arrays.asList("a.b[1].c[2]")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test(expectedExceptions = PatternException.class)
  public void singlePatternNone() {
    // GIVEN

    // WHEN
    colPatternUtil.findColNamesForColNamePattern(env, "a.b[1].c[0]");

    // THEN: PatternException (no [*])
  }

  @Test
  public void multiplePatternSimple() {
    // GIVEN
    simulateLengths(new Triple<>("a.b", 5L, 2L));

    // GIVEN WHEN
    ColumnPatternContainer patterns =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.b[*].d"));
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList(Arrays.asList("a.b[0].c", "a.b[0].d"), //
        Arrays.asList("a.b[1].c", "a.b[1].d")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test(expectedExceptions = PatternException.class)
  public void multiplePatternWrongRepetition() {
    // GIVEN WHEN
    colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.c[*].d[*]"));

    // THEN - exception
  }

  @Test
  public void multiplePatternTwoLevel() {
    // GIVEN
    simulateLengths(new Triple<>("a.b", 3L, 3L), //
        new Triple<>("a.b[0].d", 7L, 3L), //
        new Triple<>("a.b[1].d", 2L, 1L), //
        new Triple<>("a.b[2].d", 1L, 0L));

    // WHEN
    ColumnPatternContainer patterns =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.b[*].d[*].e"));
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a.b[0].c", "a.b[0].d[0].e"), //
        Arrays.asList("a.b[0].c", "a.b[0].d[1].e"), //
        Arrays.asList("a.b[0].c", "a.b[0].d[2].e"), //
        Arrays.asList("a.b[1].c", "a.b[1].d[0].e") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoLevel2() {
    // GIVEN
    simulateLengths(new Triple<>("a", 3L, 2L), //
        new Triple<>("a[0].b.c", 3L, 2L), //
        new Triple<>("a[1].b.c", 3L, 1L), //
        new Triple<>("a[2].b.c", 1L, 1L) //
    );

    // WHEN
    ColumnPatternContainer patterns =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a[*].b.d"));
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a[0].b.d"), //
        Arrays.asList("a[0].b.c[1]", "a[0].b.d"), //
        Arrays.asList("a[1].b.c[0]", "a[1].b.d") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoLevel3() {
    // GIVEN
    simulateLengths(new Triple<>("a", 3L, 2L), //
        new Triple<>("a[0].b.c", 3L, 2L), //
        new Triple<>("a[1].b.c", 3L, 1L), //
        new Triple<>("a[2].b.c", 3L, 2L) //
    );

    // WHEN
    ColumnPatternContainer patterns =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a[*].b.c[*]"));
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a[0].b.c[0]"), //
        Arrays.asList("a[0].b.c[1]", "a[0].b.c[1]"), //
        Arrays.asList("a[1].b.c[0]", "a[1].b.c[0]")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoAndNone() {
    // GIVEN
    simulateLengths(new Triple<>("a", 3L, 2L), //
        new Triple<>("a[0].b.c", 3L, 2L), //
        new Triple<>("a[1].b.c", 3L, 1L), //
        new Triple<>("a[2].b.c", 3L, 2L) //
    );

    // WHEN
    ColumnPatternContainer patterns =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a.x"));
    Set<List<String>> res = patterns.getColumnPatterns(1L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a.x"), //
        Arrays.asList("a[0].b.c[1]", "a.x"), //
        Arrays.asList("a[1].b.c[0]", "a.x")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  /**
   * @param lengths
   *          Triple of "colName", "maxLen", "len of requested row".
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SafeVarargs
  private final void simulateLengths(Triple<String, Long, Long>... lengths) {
    for (Triple<String, Long, Long> lengthTriple : lengths) {
      String colName = lengthTriple.getLeft();
      long maxLen = lengthTriple.getMiddle();
      long len = lengthTriple.getRight();
      LongDictionary<?> colDictMock = Mockito.mock(LongDictionary.class);
      Mockito.when(colDictMock.getMaxId()).thenReturn(Long.valueOf(Long.MIN_VALUE));
      Mockito.when(colDictMock.decompressValue(Mockito.anyLong())).thenAnswer(new Answer<Long>() {
        @Override
        public Long answer(InvocationOnMock invocation) throws Throwable {
          if (invocation.getArguments()[0].equals(Long.MIN_VALUE))
            return Long.valueOf(maxLen);
          return Long.valueOf(len);
        }
      });

      QueryableLongColumnShard lenColMock = Mockito.mock(QueryableLongColumnShard.class, Mockito.RETURNS_MOCKS);
      Mockito.when(lenColMock.getColumnShardDictionary()).thenReturn((LongDictionary) colDictMock);
      // any value != Long.MIN_VALUE.
      Mockito.when(lenColMock.resolveColumnValueIdForRow(Mockito.anyLong())).thenReturn(99L);

      Mockito.when(env.getLongColumnShard(new RepeatedColumnNameGenerator().repeatedLength(colName)))
          .thenReturn(lenColMock);
    }
  }

}
