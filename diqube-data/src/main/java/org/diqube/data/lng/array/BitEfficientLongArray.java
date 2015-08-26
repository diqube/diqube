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
package org.diqube.data.lng.array;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayBitEfficient;

/**
 * A {@link CompressedLongArray} that stores the long values in a bit-efficient way.
 * 
 * <p>
 * This means that it inspects the uncompressed values for min and max values and determines the number of bits actually
 * needed to represent all of those values. It then stores these number of bits per entry in the array.
 * 
 * <p>
 * Internal representation of these bits is another long array, where each of the input values is represented with a
 * leading bit that designates positive or a negative value (only if negative values are at all int he input array)
 * followed by the bit representation of the absolute value of the input value. This representation forces to take
 * special care about values {@link Long#MIN_VALUE}, because these are internally represented using two' complement and
 * therefore cannot be represented with max 64 bits in our representation.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SLongCompressedArrayBitEfficient.class)
public class BitEfficientLongArray extends AbstractExplorableCompressedLongArray<SLongCompressedArrayBitEfficient> {

  /** Number of elements in the array. Available after {@link #prepareCompression(long[], boolean)}. */
  private int size;
  /**
   * Number of bits used per compressed value, including potential sign-bits. Available after
   * {@link #prepareCompression(long[], boolean)}.
   */
  private int numberOfBitsPerValue;
  /**
   * true if input array was sorted, and compressed version of elements is sorted, too, therefore. Available after
   * {@link #prepareCompression(long[], boolean)}.
   */
  private boolean isSorted;
  /**
   * true if the uncompressed version of the array contains the same value for each index. Available after
   * {@link #prepareCompression(long[], boolean)}.
   */
  private boolean isSameValue;
  /**
   * true if our compression contains a sign-bit first. Available after {@link #prepareCompression(long[], boolean)}.
   */
  private boolean containsSignBit;
  /**
   * The array containing the bit-compressed values. This does not contain the {@link Long#MIN_VALUE} elements of the
   * uncompressed input array at all. This array may be <code>null</code> if the input array contained only
   * {@link Long#MIN_VALUE}s or {@link #size} == 0.
   */
  private long[] compressedValues;
  /**
   * Indices of the input array where {@link Long#MIN_VALUE} was found. This array is sorted, but may be
   * <code>null</code> in case there were no {@link Long#MIN_VALUE}s in the input array or {@link #size} == 0.
   */
  private int[] longMinValueLocations;
  /** Number of {@link Long#MIN_VALUE}s in input array. Available after {@link #prepareCompression(long[], boolean)}. */
  private int numberOfLongMinValues;
  /**
   * Minimum decompressed value, not counting {@link Long#MIN_VALUE}. Available after
   * {@link #prepareCompression(long[], boolean)}.
   */
  private long minValue;
  /**
   * Minimum decompressed value, counting {@link Long#MIN_VALUE}. Available after
   * {@link #prepareCompression(long[], boolean)}.
   */
  private long absoluteMinValue;
  /** Maximum decompressed value. Available after {@link #prepareCompression(long[], boolean)}. */
  private long maxValue;

  /**
   * Instantiate new {@link BitEfficientLongArray} and execute compression right away.
   * 
   * @param originalValues
   *          The uncompressed values.
   * @param isSorted
   *          true if the values are sorted.
   */
  public BitEfficientLongArray(long[] originalValues, boolean isSorted) {
    super();
    compress(originalValues, isSorted);
  }

  /**
   * Instantiate new {@link BitEfficientLongArray} in {@link State#EXPLORING}.
   */
  public BitEfficientLongArray() {
    super();
  }

  @Override
  protected double doExpectedCompressionRatio(long[] originalValues, boolean isSorted) {
    return calculateApproxCompressionRatio(numberOfBitsPerValue, isSameValue, size, numberOfLongMinValues);
  }

  /**
   * Prepare the compression, but do not execute compression itself. After calling this method, following fields are
   * set:
   * 
   * <ul>
   * <li>{@link #isSorted}
   * <li>{@link #size}
   * <li>{@link #numberOfLongMinValues}
   * <li>{@link #absoluteMinValue}
   * <li>{@link #minValue}
   * <li>{@link #maxValue}
   * <li>{@link #isSameValue}
   * <li>{@link #numberOfBitsPerValue}
   * <li>{@link #containsSignBit}
   * </ul>
   * 
   * @param originalValues
   * @param isSorted
   */
  @Override
  protected void doPrepareCompression(long[] originalValues, boolean isSorted) {
    this.isSorted = isSorted;
    size = originalValues.length;

    if (size == 0)
      return;

    numberOfLongMinValues = 0;

    // find min and max
    if (isSorted) {
      absoluteMinValue = originalValues[0];
      maxValue = originalValues[originalValues.length - 1];
    } else {
      absoluteMinValue = Long.MAX_VALUE;
      maxValue = Long.MIN_VALUE;
      for (int i = 0; i < originalValues.length; i++) {
        if (originalValues[i] < absoluteMinValue)
          absoluteMinValue = originalValues[i];
        if (originalValues[i] > maxValue)
          maxValue = originalValues[i];
        // as we traverse the whole array anyway, we can count the number of MIN_VALUES we see, as we might need this
        // later.
        if (originalValues[i] == Long.MIN_VALUE)
          numberOfLongMinValues++;
      }
    }

    isSameValue = absoluteMinValue == maxValue;

    if (absoluteMinValue == Long.MIN_VALUE) {
      // Special case: We cannot represent Long.MIN_VALUE in our representation within 64 bits, we do not use two's
      // complement representation. Therefore having Long.MIN_VALUE in the input array is a special case: We store the
      // locations of the MIN_VALUEs in a separate array, and do not use the compression algorithm below for these
      // values.
      if (isSameValue) {
        // Input array has ONLY Long.MIN_VALUE
        numberOfLongMinValues = size;
        minValue = Long.MIN_VALUE;
        numberOfBitsPerValue = 0;
        return;
      }

      // Now we need to know how much times MIN_VALUE is inside the array. We know this already, if the array is not
      // sorted (counted that when traversing the array before).
      if (isSorted) {
        if (originalValues[1] != Long.MIN_VALUE)
          numberOfLongMinValues = 1;
        else {
          numberOfLongMinValues = binarySearchForLastOccurenceOfLongMinValue(originalValues);
        }
      }

      // Fill in locations of Long.MIN_VALUE & find new min value (ignoring Long.MIN_VALUE), so we will be able to
      // calculate the optimal bit-representation for all values that we need to compress later.
      if (isSorted) {
        minValue = originalValues[numberOfLongMinValues];
      } else {
        minValue = maxValue;
        for (int i = 0; i < originalValues.length; i++) {
          if (originalValues[i] != Long.MIN_VALUE && originalValues[i] < minValue)
            minValue = originalValues[i];
        }
      }
    } else
      minValue = absoluteMinValue;

    // Calculate number of bits needed per input value.
    numberOfBitsPerValue = 0;
    if (maxValue > 0) {
      numberOfBitsPerValue = numberOfBitsNeededForPositiveLong(maxValue);
    }

    containsSignBit = false;
    if (minValue < 0) {
      numberOfBitsPerValue =
          Math.max(numberOfBitsPerValue + 1, numberOfBitsNeededForPositiveLong(Math.abs(minValue)) + 1);
      containsSignBit = true;
    }

    if (numberOfBitsPerValue == 0)
      // This could happen if maxValue == minValue == 0 -> let's store at least one bit.
      numberOfBitsPerValue = 1;
  }

  @Override
  protected void doCompress(long[] originalValues, boolean isSorted) {
    if (numberOfLongMinValues > 0) {
      // now we can instantiate our array which will hold the locations of Long.MIN_VALUE.
      longMinValueLocations = new int[numberOfLongMinValues];

      // Fill in locations of Long.MIN_VALUE & find new min value (ignoring Long.MIN_VALUE), so we will be able to
      // calculate the optimal bit-representation for all values that we need to compress later.
      if (isSorted) {
        for (int i = 0; i < numberOfLongMinValues; i++)
          longMinValueLocations[i] = i;
      } else {
        int j = 0;
        for (int i = 0; i < originalValues.length; i++) {
          if (originalValues[i] == Long.MIN_VALUE)
            longMinValueLocations[j++] = i;
        }
      }

      if (isSameValue)
        // Input array has ONLY Long.MIN_VALUE
        return;
    }

    // Start compressing the values
    compressedValues = new long[(int) Math.ceil(numberOfBitsPerValue * (size - numberOfLongMinValues) / 64.)];
    Arrays.fill(compressedValues, 0L);

    int longMinValuesSeen = 0;
    for (int pos = 0; pos < originalValues.length; pos++) {
      // Skip positions with Long.MIN_VALUE, as they have been handled separately.
      if (originalValues[pos] == Long.MIN_VALUE) {
        longMinValuesSeen++;
        continue;
      }

      // calculate the bit representation we want to store. This value will at most have numberOfBitsPerValue set, at
      // bit numbers 0..numberOfBitsPerValue-1.
      long value;
      if (containsSignBit) {
        value = originalValues[pos];
        if (value < 0)
          value = -value; // make value positive, so we do not have to handle two's complement representation below.
        value = value & createBitMask(0, numberOfBitsPerValue - 1);
        if (originalValues[pos] < 0)
          value |= 1L << (numberOfBitsPerValue - 1); // set sign bit, marking this value as negative.
      } else {
        // Positive value.
        value = originalValues[pos] & createBitMask(0, numberOfBitsPerValue);
      }

      // find the long inside compressedValues where the first (or all) bits of our new value will be stored.
      int compressedPos = (int) Math.floorDiv((long) (pos - longMinValuesSeen) * numberOfBitsPerValue, 64L);

      // Uppermost bit: Bit number from the right side of the long that contains the uppermost bit of our compressed
      // value. Bit numbers are 0-based, means right-most and least-valued bit has number 0.
      int compressedPosUppermostBit = 63 - (int) (((long) (pos - longMinValuesSeen) * numberOfBitsPerValue) % 64L);
      int spaceLeftInCompressedLong = compressedPosUppermostBit + 1;

      if (spaceLeftInCompressedLong >= numberOfBitsPerValue)
        // New compressed value fits fully into the compressed long.
        compressedValues[compressedPos] |= value << (spaceLeftInCompressedLong - numberOfBitsPerValue);
      else {
        // Split value between two compressed longs.
        compressedValues[compressedPos] |= value >>> numberOfBitsPerValue - spaceLeftInCompressedLong;
        compressedValues[compressedPos + 1] |= value << (64 - numberOfBitsPerValue + spaceLeftInCompressedLong);
      }
    }
  }

  /**
   * Does a binary search on a sorted array for the last occurence of {@link Long#MIN_VALUE}.
   * 
   * Expects that the array does (1) not only contain {@link Long#MIN_VALUE} and (2) that at least the first two entries
   * in the array are {@link Long#MIN_VALUE}.
   * 
   * @return number of times the array contains {@link Long#MIN_VALUE}. These values then occur at indices 0..result-1.
   */
  private int binarySearchForLastOccurenceOfLongMinValue(long[] originalValues) {
    int numberOfLongMinValues = 0;

    int lo = 0;
    int high = originalValues.length - 1;
    while (numberOfLongMinValues == 0) {
      int mid = lo + Math.floorDiv(high - lo, 2);
      if (originalValues[mid] == Long.MIN_VALUE) {
        if (originalValues[mid + 1] != Long.MIN_VALUE) {
          numberOfLongMinValues = mid + 1;
          break;
        }
        lo = mid;
      } else {
        if (originalValues[mid - 1] == Long.MIN_VALUE) {
          numberOfLongMinValues = mid;
          break;
        }
        high = mid;
      }
    }
    return numberOfLongMinValues;
  }

  private static int numberOfBitsNeededForPositiveLong(long positiveValue) {
    if (positiveValue == 0)
      return 1;

    return 64 - Long.numberOfLeadingZeros(positiveValue);
  }

  @Override
  public boolean isSameValue() {
    if (size == 0)
      return true;
    return isSameValue;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isSorted() {
    return isSorted;
  }

  @Override
  public long[] decompressedArray() {
    if (size == 0)
      return new long[0];

    // function iterating linearily through longMinValueLocations and finding Long.MIN_VALUEs.
    Function<Integer, Integer> minValueFn = new Function<Integer, Integer>() {
      private int nextLongMinValueLocationIdx = 0;

      @Override
      public Integer apply(Integer t) {
        if (nextLongMinValueLocationIdx == longMinValueLocations.length)
          return longMinValueLocations.length;
        if (longMinValueLocations[nextLongMinValueLocationIdx] == t) {
          nextLongMinValueLocationIdx++;
          // value is Long.MIN_VALUE
          return null;
        }
        return nextLongMinValueLocationIdx;
      }
    };

    long[] res = new long[size];
    for (int i = 0; i < size; i++)
      res[i] = get(i, minValueFn);
    return res;
  }

  @Override
  public long get(int index) throws ArrayIndexOutOfBoundsException {
    return get(index, new Function<Integer, Integer>() {
      @Override
      public Integer apply(Integer index) {
        // Execute logarithmic binary search
        int longMinValueIndex = Arrays.binarySearch(longMinValueLocations, index);

        if (longMinValueIndex >= 0 && longMinValueLocations[longMinValueIndex] == index)
          // value at index is actually MIN_VALUE.
          return null;

        // value at index is not MIN_VALUE, return number of MIN_VALUES that have index < our index.
        int numberOfLongMinValuesBeforeCurrentPos = Math.abs(longMinValueIndex) - 1;
        return numberOfLongMinValuesBeforeCurrentPos;
      }
    });
  }

  /**
   * Executes {@link #get(int)}, but using a separate function that is capable of identifying if the value at that index
   * is {@link Long#MIN_VALUE} and if not, how many {@link Long#MIN_VALUE}s are available before that index.
   * 
   * @param minValueFn
   *          Function gets index, returns <code>null</code> if there is a {@link Long#MIN_VALUE} at this index,
   *          otherwise it returns the number of {@link Long#MIN_VALUE}s that have index < the passed index.
   */
  public long get(int index, Function<Integer, Integer> minValueFn) throws ArrayIndexOutOfBoundsException {
    if (index < 0 || index >= size)
      throw new ArrayIndexOutOfBoundsException("Tried to access index " + index + " but size is " + size);

    // If the array contains Long.MIN_VALUEs, we need to know (1) if the queried index has value Long.MIN_VALUE and if
    // not, (2) how much Long.MIN_VALUEs have been stored for indices that are smaller than the current index: We did
    // not put any bits into compressedValues for the Long.MIN_VALUE input values, we therefore need this information to
    // calculate the position of the compressed value in compressedValues we want to decompress.
    int numberOfLongMinValuesBeforeCurrentPos = 0;
    if (longMinValueLocations != null) {
      Integer minValueResult = minValueFn.apply(index);
      if (minValueResult == null)
        return Long.MIN_VALUE;
      numberOfLongMinValuesBeforeCurrentPos = minValueResult;
    }

    // Calculate the long in compressedValues that holds the first (or all) bits of the value we want to load.
    int compressedPos =
        (int) Math.floorDiv((long) (index - numberOfLongMinValuesBeforeCurrentPos) * numberOfBitsPerValue, 64L);

    // Uppermost bit: Bit number from the right side of the long that contains the uppermost bit of our compressed
    // value. Bit numbers are 0-based, means right-most and least-valued bit has number 0.
    int compressedPosUppermostBit =
        63 - (int) (((long) (index - numberOfLongMinValuesBeforeCurrentPos) * numberOfBitsPerValue) % 64L);
    int numberOfBitsStoredInCompressedLong = compressedPosUppermostBit + 1;

    long value;
    if (numberOfBitsStoredInCompressedLong >= numberOfBitsPerValue) {
      // Compressed value is fully contained in one compressedValue[] entry.
      value = compressedValues[compressedPos]
          & createBitMask(compressedPosUppermostBit - numberOfBitsPerValue + 1, compressedPosUppermostBit);
      value = value >>> (compressedPosUppermostBit - numberOfBitsPerValue + 1);
    } else {
      // Compressed value is split across two compressedValue[] entries.
      // upper bits
      value = compressedValues[compressedPos] & createBitMask(0, compressedPosUppermostBit);
      value = value << (numberOfBitsPerValue - compressedPosUppermostBit - 1);
      // lower bits
      int numberOfLowerBits = numberOfBitsPerValue - numberOfBitsStoredInCompressedLong;
      long lowerBits = compressedValues[compressedPos + 1] & createBitMask(64 - numberOfLowerBits, 63);
      value |= lowerBits >>> (64 - numberOfLowerBits);
    }

    // Check if the compressed value is negative.
    boolean uppermostBitSet = (value & (1L << (numberOfBitsPerValue - 1))) != 0;
    if (containsSignBit && uppermostBitSet) {
      value &= ~(1L << (numberOfBitsPerValue - 1)); // unset uppermost bit
      value = -value;
    }

    return value;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SLongCompressedArrayBitEfficient target)
      throws SerializationException {

    target.setSize(size);
    target.setNumberOfBitsPerValue(numberOfBitsPerValue);
    target.setIsSorted(isSorted);
    target.setIsSameValue(isSameValue);
    target.setContainsSignBit(containsSignBit);
    target.setMinValue(minValue);
    target.setAbsoluteMinValue(absoluteMinValue);
    target.setMaxValue(maxValue);
    if (longMinValueLocations != null)
      target.setLongMinValueLocations(IntStream.of(longMinValueLocations).boxed().collect(Collectors.toList()));
    target.setCompressedValues(LongStream.of(compressedValues).boxed().collect(Collectors.toList()));
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SLongCompressedArrayBitEfficient source)
      throws DeserializationException {
    size = source.getSize();
    numberOfBitsPerValue = source.getNumberOfBitsPerValue();
    isSorted = source.isIsSorted();
    isSameValue = source.isIsSameValue();
    containsSignBit = source.isContainsSignBit();
    compressedValues = source.getCompressedValues().stream().mapToLong(Long::longValue).toArray();
    if (source.isSetLongMinValueLocations())
      longMinValueLocations = source.getLongMinValueLocations().stream().mapToInt(Integer::intValue).toArray();
    numberOfLongMinValues = (longMinValueLocations != null) ? longMinValueLocations.length : 0;
    minValue = source.getMinValue();
    absoluteMinValue = source.getAbsoluteMinValue();
    maxValue = source.getMaxValue();
  }

  /**
   * @return A long having the bits at the specific locations set (lower- and upper-bound indices included)
   */
  private long createBitMask(int idxLowestBitSet, int idxUppermostBitSet) {
    long res = 0;
    for (int i = idxLowestBitSet; i <= idxUppermostBitSet; i++)
      res |= 1L << i;
    return res;
  }

  private static double calculateApproxCompressionRatio(int numberOfBitsPerValue, boolean isSameValue, int size,
      int numberOfLongMinValues) {
    if (size == 0)
      return 1.;
    if (numberOfLongMinValues > 0 && isSameValue)
      // Only MIN_VALUEs.
      return 1.;

    double avgBitsPerEntryCompressed =
        ((((double) size - numberOfLongMinValues) * numberOfBitsPerValue) + numberOfLongMinValues * 64) / size;

    double uncompressedNumberOfBitsPerValue = 64.;

    return avgBitsPerEntryCompressed / uncompressedNumberOfBitsPerValue;
  }

  /**
   * Calculate an approximate compression ratio based not on a full array (use
   * {@link #expectedCompressionRatio(long[], boolean)} for that), but based on a few values calculated for an input
   * array.
   * 
   * @param min
   *          Minimum value of the input array. If the array contains {@link Long#MIN_VALUE}, this parameter should
   *          contain the value of the <b>second smallest</b> number in the array (= ignore {@link Long#MIN_VALUE}!).
   *          This parameter is not expected to be set to {@link Long#MIN_VALUE}. This value is ignored if the input
   *          array only contains {@link Long#MIN_VALUE} (-> size == numberOfLongMinValues).
   * @param max
   *          Maximum value of the input array. This value is ignored if the input array only contains
   *          {@link Long#MIN_VALUE} (-> size == numberOfLongMinValues).
   * @param size
   *          Overall number of elements in the input array.
   * @param numberOfLongMinValues
   *          The number of times the input array contains the value {@link Long#MIN_VALUE}.
   * @return Compression ratio as defined as result in {@link #expectedCompressionRatio(long[], boolean)}.
   */
  public static double calculateApproxCompressionRatio(long min, long max, int size, int numberOfLongMinValues) {
    if (size == numberOfLongMinValues)
      return 1.;
    int numberOfBitsPerMinValue = numberOfBitsNeededForPositiveLong(Math.abs(min)) + ((min < 0) ? 1 : 0);
    int numberOfBitsPerMaxValue = numberOfBitsNeededForPositiveLong(Math.abs(max)) + ((max < 0) ? 1 : 0);
    boolean sameValue = min == max && numberOfLongMinValues == 0;
    return calculateApproxCompressionRatio(Math.max(1, Math.max(numberOfBitsPerMinValue, numberOfBitsPerMaxValue)),
        sameValue, size, numberOfLongMinValues);
  }

}
