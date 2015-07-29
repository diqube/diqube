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
package org.diqube.execution.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.util.ColumnPatternUtil.PatternException;
import org.mockito.Mockito;
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
    colPatternUtil.initialize();
    env = Mockito.mock(ExecutionEnvironment.class, Mockito.RETURNS_MOCKS);
  }

  @Test
  public void singlePatternSimple() {
    // GIVEN WHEN
    Set<String> res = colPatternUtil.findColNamesForColNamePattern(env, "a.b[*].c", colShard -> 2L);

    // THEN
    Set<String> expected = new HashSet<>(Arrays.asList("a.b[0].c", "a.b[1].c"));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void singlePatternSimple2() {
    // GIVEN WHEN
    Set<String> res = colPatternUtil.findColNamesForColNamePattern(env, "a.b[*].c[*]", colShard -> 2L);

    // THEN
    Set<String> expected = new HashSet<>(Arrays.asList("a.b[0].c[0]", "a.b[0].c[1]", "a.b[1].c[0]", "a.b[1].c[1]"));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void singlePatternNone() {
    // GIVEN WHEN
    Set<String> res = colPatternUtil.findColNamesForColNamePattern(env, "a.b[1].c[0]", colShard -> 2L);

    // THEN
    Set<String> expected = new HashSet<>(Arrays.asList("a.b[1].c[0]"));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternSimple() {
    // GIVEN WHEN
    Set<List<String>> res =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.b[*].d"), colShard -> 2L);

    // THEN
    Set<List<String>> expected =
        new HashSet<>(Arrays.asList(Arrays.asList("a.b[0].c", "a.b[0].d"), Arrays.asList("a.b[1].c", "a.b[1].d")));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test(expectedExceptions = PatternException.class)
  public void multiplePatternWrongRepetition() {
    // GIVEN WHEN
    colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.c[*].d[*]"), colShard -> 2L);

    // THEN - exception
  }

  @Test
  public void multiplePatternTwoLevel() {
    // GIVEN WHEN
    Set<List<String>> res =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a.b[*].c", "a.b[*].d[*].e"), colShard -> 2L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a.b[0].c", "a.b[0].d[0].e"), //
        Arrays.asList("a.b[0].c", "a.b[0].d[1].e"), //
        Arrays.asList("a.b[1].c", "a.b[1].d[0].e"), //
        Arrays.asList("a.b[1].c", "a.b[1].d[1].e") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoLevel2() {
    // GIVEN WHEN
    Set<List<String>> res =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a[*].b.d"), colShard -> 2L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a[0].b.d"), //
        Arrays.asList("a[0].b.c[1]", "a[0].b.d"), //
        Arrays.asList("a[1].b.c[0]", "a[1].b.d"), //
        Arrays.asList("a[1].b.c[1]", "a[1].b.d") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoLevel3() {
    // GIVEN WHEN
    Set<List<String>> res =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a[*].b.c[*]"), colShard -> 2L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a[0].b.c[0]"), //
        Arrays.asList("a[0].b.c[1]", "a[0].b.c[1]"), //
        Arrays.asList("a[1].b.c[0]", "a[1].b.c[0]"), //
        Arrays.asList("a[1].b.c[1]", "a[1].b.c[1]") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

  @Test
  public void multiplePatternTwoAndNone() {
    // GIVEN WHEN
    Set<List<String>> res =
        colPatternUtil.findColNamesForColNamePattern(env, Arrays.asList("a[*].b.c[*]", "a.x"), colShard -> 2L);

    // THEN
    Set<List<String>> expected = new HashSet<>(Arrays.asList( //
        Arrays.asList("a[0].b.c[0]", "a.x"), //
        Arrays.asList("a[0].b.c[1]", "a.x"), //
        Arrays.asList("a[1].b.c[0]", "a.x"), //
        Arrays.asList("a[1].b.c[1]", "a.x") //
    ));

    Assert.assertEquals(res, expected, "Expected correct final colNames");
  }

}
