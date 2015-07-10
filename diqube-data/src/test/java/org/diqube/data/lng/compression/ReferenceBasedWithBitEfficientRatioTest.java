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

import org.diqube.data.lng.array.BitEfficientLongArray;
import org.diqube.data.lng.array.ReferenceBasedLongArray;
import org.diqube.data.lng.array.TransitiveExplorableCompressedLongArray.TransitiveCompressionRatioCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests compression ratios calculated by a combination of {@link ReferenceBasedLongArray} with a
 * {@link BitEfficientLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class ReferenceBasedWithBitEfficientRatioTest {
  @Test
  public void bitwiseTest() {
    for (int noBits = 1; noBits <= 63; noBits++) {
      // GIVEN
      // Values having one bit set
      long[] inputArray = new long[noBits];
      inputArray[0] = 1;
      for (int i = 1; i < inputArray.length; i++)
        inputArray[i] = inputArray[i - 1] << 1;

      assertRatio(inputArray, (noBits == 1) ? 1 : (noBits - 1), " (bits=" + noBits + ")");
    }
  }

  @Test
  public void withMinValueZeroTest() {
    long[] inputArray = new long[] { Long.MIN_VALUE, 0 };
    double avgNumberOfBitsPerValue = 64.;

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  @Test
  public void withMinValueZeroPlus1Test() {
    long[] inputArray = new long[] { Long.MIN_VALUE + 1, 0 };
    double avgNumberOfBitsPerValue = 63.;

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  @Test
  public void withMaxValueZeroTest() {
    long[] inputArray = new long[] { 0, Long.MAX_VALUE };
    double avgNumberOfBitsPerValue = 63.;

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  @Test
  public void withMaxValueAndMinusOneTest() {
    long[] inputArray = new long[] { Long.MAX_VALUE - 1, Long.MAX_VALUE };
    double avgNumberOfBitsPerValue = 1.; // 0 and 1 are encoded in BitEfficient

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  @Test
  public void withMinPlusOneAndTwoValueTest() {
    long[] inputArray = new long[] { Long.MIN_VALUE + 1, Long.MIN_VALUE + 2 };
    double avgNumberOfBitsPerValue = 1.; // 0 and 1 are encoded in BitEfficient

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  @Test
  public void withMinusOneToOneTest() {
    long[] inputArray = new long[] { -1, 0, 1 };
    double avgNumberOfBitsPerValue = 2.;

    assertRatio(inputArray, avgNumberOfBitsPerValue, null);
  }

  /**
   * Assert compression ratio with no MIN_VALUES in the input array
   * 
   * @param inputArray
   * @param avgNumberOfBitsPerValue
   * @param assertionText
   */
  private void assertRatio(long[] inputArray, double avgNumberOfBitsPerValue, String assertionText) {
    TransitiveCompressionRatioCalculator calculator = new TransitiveCompressionRatioCalculator() {
      @Override
      public double calculateTransitiveCompressionRatio(long min, long secondMin, long max, long size) {
        return BitEfficientLongArray.calculateApproxCompressionRatio(min, max, (int) size, 0);
      }
    };

    ReferenceBasedLongArray longArray = new ReferenceBasedLongArray();
    double ratioSorted = longArray.expectedCompressionRatio(inputArray, true, calculator);
    double ratioUnsorted = longArray.expectedCompressionRatio(inputArray, false, calculator);

    String txt = "Sorted ratio should be equal to unsorted";
    if (assertionText != null)
      txt += assertionText;
    Assert.assertEquals(ratioUnsorted, ratioSorted, 0.001, txt);
    txt = "Expected different ratio";
    if (assertionText != null)
      txt += assertionText;
    Assert.assertEquals(ratioUnsorted, avgNumberOfBitsPerValue / 64., 0.001, txt);
  }

}
