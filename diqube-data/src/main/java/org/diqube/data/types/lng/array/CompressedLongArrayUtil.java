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
package org.diqube.data.types.lng.array;

import java.util.Arrays;

/**
 * Utility methods for {@link CompressedLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class CompressedLongArrayUtil {
  /**
   * Execute a binary search on a sorted {@link CompressedLongArray}.
   * 
   * <p>
   * Searches the whole array for the given value.
   * 
   * <p>
   * Note that this method does only make sense to be executed on constant-access-time implementations of
   * {@link CompressedLongArray} (i.e. not on {@link RunLengthLongArray}).
   * 
   * @return Index >= 0 if found. If not found, the result is (-1-instertionpoint), means < 0 and it might be -(high+2).
   *         This is equal to the result of {@link Arrays#binarySearch(long[], long)}.
   */
  public static int binarySearch(CompressedLongArray<?> sortedArray, long value) {
    if (sortedArray.size() == 0)
      return -1 - 0; // Insertion point 0

    return binarySearch(sortedArray, value, 0, sortedArray.size() - 1);
  }

  /**
   * Execute a binary search on a sub-array of a sorted {@link CompressedLongArray}.
   *
   * <p>
   * Note that this method does only make sense to be executed on constant-access-time implementations of
   * {@link CompressedLongArray} (i.e. not on {@link RunLengthLongArray}).
   * 
   * @param lo
   *          lowest index to start search (inclusive).
   * @param hi
   *          highest index to start search (inclusive).
   * @return Index >= 0 if found. If not found, the result is (-1-instertionpoint), means < 0 and it might be -(high+2).
   *         This is equal to the result of {@link Arrays#binarySearch(long[], long)}.
   */
  private static int binarySearch(CompressedLongArray<?> sortedArray, long value, int lo, int high) {
    // TODO #5 instead of decompressing a lot of values here, we should compress the search value itself and then search
    // for it in the compressed space. This should be implemented in CompressedLongArray.
    long decompressed = sortedArray.get(lo);
    if (decompressed == value)
      return lo;
    if (decompressed > value)
      return -1;
    decompressed = sortedArray.get(high);
    if (decompressed == value)
      return high;
    if (decompressed < value)
      return -2 - high;
    while (true) {
      if (high - lo <= 10) {
        for (int i = lo + 1; i <= high - 1; i++) {
          decompressed = sortedArray.get(i);
          if (decompressed == value)
            return i;
          if (decompressed > value) {
            return -1 - i;
          }
        }
        return -1 - high;
      }
      int mid = lo + ((high - lo) / 2);
      long midValue = sortedArray.get(mid);
      if (midValue == value)
        return mid;
      if (midValue > value)
        high = mid;
      else
        lo = mid;
    }
  }

}
