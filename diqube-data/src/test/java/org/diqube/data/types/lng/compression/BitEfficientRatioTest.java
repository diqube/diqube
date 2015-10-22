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
package org.diqube.data.types.lng.compression;

import org.diqube.data.types.lng.array.BitEfficientLongArray;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests compression ratios calculated by {@link BitEfficientLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class BitEfficientRatioTest {
  @Test
  public void bitwiseTest() {
    for (int noBits = 1; noBits <= 63; noBits++) {
      // GIVEN
      // Values having one bit set
      long[] inputArray = new long[noBits];
      inputArray[0] = 1;
      for (int i = 1; i < inputArray.length; i++)
        inputArray[i] = inputArray[i - 1] << 1;

      BitEfficientLongArray longArray = new BitEfficientLongArray();
      double ratioSorted = longArray.expectedCompressionRatio(inputArray, true);
      double ratioUnsorted = longArray.expectedCompressionRatio(inputArray, false);
      double ratioStatic = BitEfficientLongArray.calculateApproxCompressionRatio(1, inputArray[inputArray.length - 1],
          inputArray.length, 0);

      Assert.assertEquals(ratioUnsorted, ratioSorted, 0.001,
          "Sorted ratio should be equal to unsorted (bits=" + noBits + ")");
      Assert.assertEquals(ratioStatic, ratioUnsorted, 0.001,
          "Expected ratio to be equal with static method (bits=" + noBits + ")");
      Assert.assertEquals(ratioUnsorted, noBits / 64., 0.001, "Expected different ratio (bits=" + noBits + ")");
    }
  }

  @Test
  public void withMinValueTest() {
    long[] inputArray = new long[] { Long.MIN_VALUE, 3 };
    double avgNumberOfBitsPerValue = (2 + 64) / 2.;

    BitEfficientLongArray longArray = new BitEfficientLongArray();
    double ratioSorted = longArray.expectedCompressionRatio(inputArray, true);
    double ratioUnsorted = longArray.expectedCompressionRatio(inputArray, false);
    double ratioStatic = BitEfficientLongArray.calculateApproxCompressionRatio(3, 3, inputArray.length, 1);

    Assert.assertEquals(ratioUnsorted, ratioSorted, 0.001, "Sorted ratio should be equal to unsorted");
    Assert.assertEquals(ratioStatic, ratioUnsorted, 0.001, "Expected ratio to be equal with static method");
    Assert.assertEquals(ratioUnsorted, avgNumberOfBitsPerValue / 64., 0.001, "Expected different ratio");
  }
}
