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
