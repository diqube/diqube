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
import java.util.function.Supplier;

import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.lng.array.ExplorableCompressedLongArray;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.internal.junit.ArrayComparisonFailure;

/**
 * Abstract implementation of {@link CompressedLongArray} tests.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractCompressedLongArrayTest {

  private Supplier<ExplorableCompressedLongArray<?>> longArraySupplier;
  private TestCompressor compressor;

  public AbstractCompressedLongArrayTest(Supplier<ExplorableCompressedLongArray<?>> longArraySupplier,
      TestCompressor compressor) {
    this.longArraySupplier = longArraySupplier;
    this.compressor = compressor;
  }

  public AbstractCompressedLongArrayTest(Supplier<ExplorableCompressedLongArray<?>> longArraySupplier) {
    this(longArraySupplier, (longArray, values, isSorted) -> longArray.compress(values, isSorted));
  }

  @Test
  public void exponentTest() {
    // GIVEN
    // arrays 0..1, 0..2, 0..4, 0..2^n up until n=63
    // array containing 2^n for n=0..62
    for (int exponent = 0; exponent <= 63; exponent++) {
      long[] values = new long[exponent + 1];
      values[0] = 1;
      for (int i = 1; i < values.length; i++) {
        values[i] = values[i - 1] << 1;
      }

      executeCompressionAndValidate(values, " (exponent == " + exponent + ")");
    }
  }

  @Test
  public void exponentNegativeTest() {
    // GIVEN
    // arrays 0..1, 0..2, 0..4, 0..2^n up until n=63
    // array containing 2^n for n=0..62
    for (int exponent = 0; exponent <= 63; exponent++) {
      long[] values = new long[2 * (exponent + 1)];
      values[0] = 1;
      values[exponent] = -1;
      for (int i = 1; i <= exponent; i++) {
        values[i] = values[i - 1] << 1;
        values[exponent + 1 + i] = values[exponent + i] << 1;
      }

      executeCompressionAndValidate(values, " (exponent == " + exponent + ")");
    }
  }

  @Test
  public void to63Test() {
    // GIVEN
    // array containing 0..63
    long[] values = new long[64];
    for (int i = 0; i < 64; i++) {
      values[i] = i;
    }

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minus63Test() {
    // GIVEN
    // array containing -63..63
    long[] values = new long[2 * 64 + 1];
    values[0] = -63;
    for (int i = 1; i < values.length; i++) {
      values[i] = values[i - 1] + 1;
    }

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minus128Test() {
    // GIVEN
    // array containing -63..63
    long[] values = new long[2 * 129 + 1];
    values[0] = -128;
    for (int i = 1; i < values.length; i++) {
      values[i] = values[i - 1] + 1;
    }

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minValueOnlyTest() {
    // GIVEN
    long[] values = new long[] { Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minValueAndZeroTest() {
    // GIVEN
    long[] values = new long[] { Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0 };

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void zeroTest() {
    // GIVEN
    long[] values = new long[] { 0, 0 };

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minMaxValueTest() {
    // GIVEN
    long[] values = new long[] { Long.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, 0 };

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void minValuePlus1000Test() {
    // GIVEN
    long[] values = new long[1001];
    values[0] = Long.MIN_VALUE;
    for (int i = 1; i < values.length; i++)
      values[i] = values[i - 1] + 1;

    executeCompressionAndValidate(values, null);
  }

  @Test
  public void maxValueMinus1000Test() {
    // GIVEN
    long[] values = new long[1001];
    values[0] = Long.MAX_VALUE;
    for (int i = 1; i < values.length; i++)
      values[i] = values[i - 1] - 1;

    executeCompressionAndValidate(values, null);
  }

  private void executeCompressionAndValidate(long[] values, String assertionText) throws ArrayComparisonFailure {
    ExplorableCompressedLongArray<?> longArray = longArraySupplier.get();
    compressor.compress(longArray, values, false);
    long[] decompressed = longArray.decompressedArray();

    String txt = "Decompressed (sorted = false) values should match original ones";
    if (assertionText != null)
      txt += assertionText;
    Assert.assertEquals(decompressed, values, txt);

    long[] decompressedSingleValues = new long[values.length];
    for (int i = 0; i < values.length; i++)
      decompressedSingleValues[i] = longArray.get(i);
    Assert.assertEquals(decompressedSingleValues, decompressed,
        "Expected get(i) to return same values as decompressedArray()");

    // make sure values are sorted and run again with sorted = true
    Arrays.sort(values);
    longArray = longArraySupplier.get();
    compressor.compress(longArray, values, true);
    decompressed = longArray.decompressedArray();

    txt = "Decompressed (sorted = true) values should match original ones";
    if (assertionText != null)
      txt += assertionText;
    Assert.assertEquals(decompressed, values, txt);

    decompressedSingleValues = new long[values.length];
    for (int i = 0; i < values.length; i++)
      decompressedSingleValues[i] = longArray.get(i);
    Assert.assertEquals(decompressedSingleValues, decompressed,
        "Expected get(i) to return same values as decompressedArray() - sorted");
  }

  protected static interface TestCompressor {
    public void compress(ExplorableCompressedLongArray<?> longArray, long[] values, boolean isSorted);
  }

}
