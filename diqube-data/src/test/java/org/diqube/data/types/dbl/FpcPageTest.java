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

import org.diqube.data.types.dbl.dict.FpcPage;
import org.diqube.data.types.dbl.dict.FpcPage.State;
import org.diqube.util.DoubleUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for {@link FpcPage}.
 *
 * @author Bastian Gloeckle
 */
public class FpcPageTest {
  @Test
  public void simpleTest() {
    // GIVEN
    FpcPage page = new FpcPage(0L);

    // WHEN
    page.compress(new double[] { -1, 0, 1, 1.5, 9999.99 });

    // THEN
    assertDblEquals(-1, page.get(0));
    assertDblEquals(0, page.get(1));
    assertDblEquals(1, page.get(2));
    assertDblEquals(1.5, page.get(3));
    assertDblEquals(9999.99, page.get(4));

    assertArrayResultCorrect(page, 4);
  }

  @Test
  public void twoPagesTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);

    // WHEN
    State stateEndPage1 = page1.compress(new double[] { -1.01, -1, 0, 1, 1.5, 9999.99 });

    FpcPage page2 = new FpcPage(0L, stateEndPage1);

    page2.compress(new double[] { 10000., 10000.01, 10000.02 });

    // THEN
    assertDblEquals(-1.01, page1.get(0));
    assertDblEquals(-1, page1.get(1));
    assertDblEquals(0, page1.get(2));
    assertDblEquals(1, page1.get(3));
    assertDblEquals(1.5, page1.get(4));
    assertDblEquals(9999.99, page1.get(5));

    assertArrayResultCorrect(page1, 4);

    assertDblEquals(10000., page2.get(0));
    assertDblEquals(10000.01, page2.get(1));
    assertDblEquals(10000.02, page2.get(2));
    assertArrayResultCorrect(page2, 2);
  }

  @Test
  public void minMaxNanEtcTest() {
    // GIVEN
    FpcPage page1 = new FpcPage(0L);

    // WHEN
    State stateEndPage1 = page1.compress(new double[] { Double.MIN_VALUE });

    FpcPage page2 = new FpcPage(0L, stateEndPage1);
    State stateEndPage2 = page2.compress(new double[] { Double.MAX_VALUE });

    FpcPage page3 = new FpcPage(0L, stateEndPage2);
    page3.compress(new double[] { Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY });

    // THEN
    assertDblEquals(Double.MIN_VALUE, page1.get(0));
    assertArrayResultCorrect(page1, 0);

    assertDblEquals(Double.MAX_VALUE, page2.get(0));
    assertArrayResultCorrect(page2, 0);

    assertDblEquals(Double.NaN, page3.get(0));
    assertDblEquals(Double.NEGATIVE_INFINITY, page3.get(1));
    assertDblEquals(Double.POSITIVE_INFINITY, page3.get(2));
    assertArrayResultCorrect(page3, 2);
  }

  @Test
  public void fibonacciTest() {
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

    // WHEN
    State stateEndPage1 = page1.compress(fib1);

    FpcPage page2 = new FpcPage(0L, stateEndPage1);
    page2.compress(fib2);

    // THEN
    assertDblArrayEquals(fib1, page1.get(0, fib1.length - 1));

    assertDblArrayEquals(fib2, page2.get(0, fib2.length - 1));
  }

  @Test
  public void findIndexTest() {
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

    // WHEN
    State stateEndPage1 = page1.compress(fib1);

    FpcPage page2 = new FpcPage(0L, stateEndPage1);
    page2.compress(fib2);

    // THEN
    Assert.assertEquals(page1.findIndex(fib1[0]), 0);
    Assert.assertEquals(page1.findIndex(fib1[fib1.length / 2]), fib1.length / 2);
    Assert.assertEquals(page1.findIndex(fib1[fib1.length - 1]), fib1.length - 1);

    Assert.assertEquals(page2.findIndex(fib2[0]), 0);
    Assert.assertEquals(page2.findIndex(fib2[fib2.length / 2]), fib2.length / 2);
    Assert.assertEquals(page2.findIndex(fib2[fib2.length - 1]), fib2.length - 1);
  }

  private void assertArrayResultCorrect(FpcPage page, int maxIdx) {
    double[] expected = new double[maxIdx + 1];
    for (int i = 0; i <= maxIdx; i++)
      expected[i] = page.get(i);

    double[] actual = page.get(0, maxIdx);

    for (int i = 0; i < expected.length; i++)
      if (!DoubleUtil.equals(expected[i], actual[i]))
        Assert.fail("At index " + i + ": expected: " + String.format("%.2f", expected) + " but was: "
            + String.format("%.2f", actual));
  }

  private void assertDblEquals(double expected, double actual) {
    if (!DoubleUtil.equals(expected, actual))
      Assert.fail("Expected: " + String.format("%.2f", expected) + " but was: " + String.format("%.2f", actual));
  }

  private void assertDblArrayEquals(double[] expected, double[] actual) {
    for (int i = 0; i < expected.length; i++)
      if (!DoubleUtil.equals(expected[i], actual[i]))
        Assert.fail("At index " + i + " expected: " + String.format("%.2f", expected[i]) + " but was: "
            + String.format("%.2f", actual[i]));
  }
}
