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
import org.diqube.data.lng.array.RunLengthLongArray;
import org.diqube.data.lng.array.TransitiveExplorableCompressedLongArray.TransitiveCompressionRatioCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests compression ratios calculated by a combination of {@link RunLengthLongArray} with a
 * {@link BitEfficientLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class RunLengthWithBitEfficientRatioTest {

  @Test
  public void withMinValueZeroTest() {
    long[] inputArray = new long[] { Long.MIN_VALUE, 0 };
    double avgNumberOfBitsPerValue = 33.5; // BitEfficient stores '0' with 1 bit, the MIN_VALUE (special case!) with 64,
                                           // makes a avg of 32.5; additional 1 bit length encoding.

    assertRatio(inputArray, avgNumberOfBitsPerValue);
  }

  @Test
  public void withMinValueZeroPlus1Test() {
    long[] inputArray = new long[] { Long.MIN_VALUE + 1, 0 };
    double avgNumberOfBitsPerValue = 65.; // BitEfficient does not use special case for MIN_VALUE+1 -> both 0 and
                                          // MIN_VALUE+1 are represented with 64 bits, additional 1 bit length

    assertRatio(inputArray, avgNumberOfBitsPerValue);
  }

  @Test
  public void withMaxValueZeroTest() {
    long[] inputArray = new long[] { 0, Long.MAX_VALUE };
    double avgNumberOfBitsPerValue = 64.;

    assertRatio(inputArray, avgNumberOfBitsPerValue);
  }

  @Test
  public void withMinusOneToOneTest() {
    long[] inputArray = new long[] { -1, 0, 1 };
    double avgNumberOfBitsPerValue = 3.;

    assertRatio(inputArray, avgNumberOfBitsPerValue);
  }

  @Test
  public void threeZeros() {
    long[] inputArray = new long[] { 0, 0, 0, 0, 1 };
    int newLengthOfArrays = 2; // both "count" and "value" array have 2 entries.
    double avgNumberOfBitsPerValue = newLengthOfArrays * 4. / inputArray.length; // 1 bit value, 3 bits length

    assertRatio(inputArray, avgNumberOfBitsPerValue);
  }

  /**
   * Assert compression ratio
   */
  private void assertRatio(long[] inputArray, double avgNumberOfBitsPerValue) {
    TransitiveCompressionRatioCalculator calculator = new TransitiveCompressionRatioCalculator() {
      @Override
      public double calculateTransitiveCompressionRatio(long min, long secondMin, long max, long size) {
        int numberOfMinValue = 0;
        if (min == Long.MIN_VALUE) {
          numberOfMinValue++;
          min = secondMin;
        }

        return BitEfficientLongArray.calculateApproxCompressionRatio(min, max, (int) size, numberOfMinValue);
      }
    };

    RunLengthLongArray longArray = new RunLengthLongArray();
    double ratioSorted = longArray.expectedCompressionRatio(inputArray, true, calculator);
    double ratioUnsorted = longArray.expectedCompressionRatio(inputArray, false, calculator);

    Assert.assertEquals(ratioUnsorted, ratioSorted, 0.001, "Sorted ratio should be equal to unsorted");
    Assert.assertEquals(ratioUnsorted, avgNumberOfBitsPerValue / 64., 0.001, "Expected different ratio");
  }

}
