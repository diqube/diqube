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
package org.diqube.data.flatten;

import java.util.Arrays;
import java.util.stream.LongStream;

import org.diqube.data.types.lng.array.BitEfficientLongArray;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link IndexFilteringCompressedLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class IndexFilteringCompressedLongArrayTest {
  private static long[] delegateUncompressed = new long[] { 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L };

  private CompressedLongArray<?> delegate;

  @BeforeMethod
  public void before() {
    delegate = new BitEfficientLongArray(delegateUncompressed, true);
  }

  @Test
  public void filterNone() {
    // GIVEN
    IndexFilteringCompressedLongArray array =
        new IndexFilteringCompressedLongArray(delegate, new BitEfficientLongArray(new long[] {}, true), 0L);

    // WHEN/THEN
    Assert.assertEquals(array.size(), 0L, "Expected correct array size");
    Assert.assertEquals(array.decompressedArray(), new long[] {}, "Expected correct decompressed array.");
  }

  @Test
  public void filterAll() {
    // GIVEN
    IndexFilteringCompressedLongArray array = new IndexFilteringCompressedLongArray(delegate,
        new BitEfficientLongArray(LongStream.range(0, delegateUncompressed.length).toArray(), true), 0L);

    // WHEN/THEN
    Assert.assertEquals(array.size(), delegate.size(), "Expected correct array size");
    Assert.assertEquals(array.decompressedArray(), delegateUncompressed, "Expected correct decompressed array.");
    for (int i = 0; i < delegateUncompressed.length; i++)
      Assert.assertEquals(array.get(i), delegateUncompressed[i], "Expected correct decompressed value for index " + i);
    Assert.assertEquals(array.getMultiple(Arrays.asList(0, delegateUncompressed.length - 1)),
        Arrays.asList(delegateUncompressed[0], delegateUncompressed[delegateUncompressed.length - 1]),
        "Expected correct result on getMultiple.");
  }

  @Test
  public void filterOdd() {
    // GIVEN
    long[] indicesAvail = LongStream.range(0, delegateUncompressed.length).filter(l -> l % 2 == 1).toArray();
    IndexFilteringCompressedLongArray array =
        new IndexFilteringCompressedLongArray(delegate, new BitEfficientLongArray(indicesAvail, true), 0L);

    // WHEN/THEN
    Assert.assertEquals(array.size(), indicesAvail.length, "Expected correct array size");
    long[] decompressedAvail = new long[indicesAvail.length];
    for (int i = 0; i < indicesAvail.length; i++)
      decompressedAvail[i] = delegateUncompressed[(int) indicesAvail[i]];
    Assert.assertEquals(array.decompressedArray(), decompressedAvail, "Expected correct decompressed array.");

    for (int i = 0; i < indicesAvail.length; i++)
      Assert.assertEquals(array.get(i), decompressedAvail[i], "Expected correct decompressed value for index " + i);

    Assert.assertEquals(array.getMultiple(Arrays.asList(0, indicesAvail.length - 1)),
        Arrays.asList(delegateUncompressed[(int) indicesAvail[0]],
            delegateUncompressed[(int) indicesAvail[indicesAvail.length - 1]]),
        "Expected correct result on getMultiple.");
  }

  @Test
  public void filterEven() {
    // GIVEN
    long[] indicesAvail = LongStream.range(0, delegateUncompressed.length).filter(l -> l % 2 == 0).toArray();
    IndexFilteringCompressedLongArray array =
        new IndexFilteringCompressedLongArray(delegate, new BitEfficientLongArray(indicesAvail, true), 0L);

    // WHEN/THEN
    Assert.assertEquals(array.size(), indicesAvail.length, "Expected correct array size");
    long[] decompressedAvail = new long[indicesAvail.length];
    for (int i = 0; i < indicesAvail.length; i++)
      decompressedAvail[i] = delegateUncompressed[(int) indicesAvail[i]];
    Assert.assertEquals(array.decompressedArray(), decompressedAvail, "Expected correct decompressed array.");

    for (int i = 0; i < indicesAvail.length; i++)
      Assert.assertEquals(array.get(i), decompressedAvail[i], "Expected correct decompressed value for index " + i);

    Assert.assertEquals(array.getMultiple(Arrays.asList(0, indicesAvail.length - 1)),
        Arrays.asList(delegateUncompressed[(int) indicesAvail[0]],
            delegateUncompressed[(int) indicesAvail[indicesAvail.length - 1]]),
        "Expected correct result on getMultiple.");
  }
}
