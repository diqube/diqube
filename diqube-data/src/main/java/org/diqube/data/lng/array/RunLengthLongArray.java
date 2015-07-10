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

import java.util.function.Supplier;

/**
 * Run-Length-Encoding for long arrays.
 * 
 * <p>
 * A run length encoding encodes repeated values into a count and the value itself. Therefore this encoding is
 * meaningful if the input array contains long consecutive parts with the same value.
 * 
 * <p>
 * This {@link CompressedLongArray} implements {@link TransitiveExplorableCompressedLongArray} and can therefore be
 * backed by other {@link ExplorableCompressedLongArray} where two arrays would be created: One holding the counts and
 * one holding the values themselves.
 * 
 * <p>
 * Be aware that the simple {@link #get(int)} method has linear runtime in this compression!
 *
 * @author Bastian Gloeckle
 */
public class RunLengthLongArray extends AbstractTransitiveExplorableCompressedLongArray {

  /** If the decompressed array is sorted. */
  private boolean isSorted;
  private long numberOfDifferentTuples;
  private long maxValue;
  private long minValue;
  private long secondMinValue;
  private long maxCount;
  private long minCount;
  /** Contains compressed values. If <code>null</code>, the compressed values are in {@link #delegateCompressedValue} */
  private long[] compressedValues;
  /** Contains compressed counts. If <code>null</code>, the compressed values are in {@link #delegateCompressedCounts} */
  private long[] compressedCounts;
  /** Contains compressed values. If <code>null</code>, the compressed values are in {@link #compressedValues} */
  private ExplorableCompressedLongArray delegateCompressedValue = null;
  /** Contains compressed counts. If <code>null</code>, the compressed values are in {@link #compressedCounts} */
  private ExplorableCompressedLongArray delegateCompressedCounts = null;
  /** size of the uncompressed array */
  private int size;

  public RunLengthLongArray() {
    super();
  }

  public RunLengthLongArray(long[] inputArray, boolean isSorted) {
    super();
    compress(inputArray, isSorted);
  }

  @Override
  protected void doPrepareCompression(long[] inputArray, boolean isSorted) {
    this.isSorted = isSorted;
    size = inputArray.length;

    maxValue = Long.MIN_VALUE;
    minValue = Long.MAX_VALUE;
    secondMinValue = Long.MAX_VALUE;

    maxCount = Long.MIN_VALUE;
    minCount = Long.MAX_VALUE;

    numberOfDifferentTuples = 1;

    long lastValue;
    long lastCount = 0;
    lastValue = inputArray[0];
    for (int pos = 0; pos < inputArray.length; pos++) {
      if (inputArray[pos] == lastValue)
        lastCount++;
      else {
        lastValue = inputArray[pos];
        lastCount = 1;
        numberOfDifferentTuples++;
      }

      if (lastValue > maxValue)
        maxValue = lastValue;
      if (lastValue < minValue) {
        if (minValue < secondMinValue)
          secondMinValue = minValue;
        minValue = lastValue;
      }
      if (lastCount > maxCount)
        maxCount = lastCount;
      if (lastCount < minCount) {
        minCount = lastCount;
      }
      if (lastValue < secondMinValue && lastValue > minValue)
        secondMinValue = lastValue;
    }
  }

  @Override
  protected double doTransitiveExpectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator) {
    double valueCompression =
        transitiveCalculator.calculateTransitiveCompressionRatio(minValue, secondMinValue, maxValue,
            numberOfDifferentTuples);
    double countCompression =
        transitiveCalculator.calculateTransitiveCompressionRatio(minCount, minCount, maxCount, numberOfDifferentTuples);

    return valueCompression + countCompression;
  }

  @Override
  protected double doExpectedCompressionRatio(long[] inputArray, boolean isSorted) {
    return (numberOfDifferentTuples * 2) / inputArray.length;
  }

  @Override
  protected void doCompress(long[] inputArray, boolean isSorted) {
    compressedValues = new long[(int) numberOfDifferentTuples];
    compressedCounts = new long[(int) numberOfDifferentTuples];

    int resultPos = 0;

    long lastValue;
    long lastCount = 0;
    lastValue = inputArray[0];
    for (int pos = 0; pos < inputArray.length; pos++) {
      if (inputArray[pos] == lastValue)
        lastCount++;
      else {
        compressedCounts[resultPos] = lastCount;
        compressedValues[resultPos] = lastValue;
        resultPos++;

        lastValue = inputArray[pos];
        lastCount = 1;
      }
    }
    compressedCounts[resultPos] = lastCount;
    compressedValues[resultPos] = lastValue;
  }

  @Override
  public void compress(long[] inputArray, boolean isSorted, Supplier<ExplorableCompressedLongArray> transitiveSupplier)
      throws IllegalStateException {
    compress(inputArray, isSorted);
    delegateCompressedCounts = transitiveSupplier.get();
    delegateCompressedCounts.compress(compressedCounts, false);
    compressedCounts = null;
    delegateCompressedValue = transitiveSupplier.get();
    delegateCompressedValue.compress(compressedValues, isSorted);
    compressedValues = null;
  }

  @Override
  public boolean isSameValue() {
    return numberOfDifferentTuples == 1;
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
    int internalSize;
    if (compressedCounts != null)
      internalSize = compressedCounts.length;
    else
      internalSize = delegateCompressedCounts.size();

    long[] res = new long[size];
    int resPos = 0;
    for (int internalPos = 0; internalPos < internalSize; internalPos++) {
      long count;
      long value;
      if (compressedCounts != null) {
        count = compressedCounts[internalPos];
        value = compressedValues[internalPos];
      } else {
        count = delegateCompressedCounts.get(internalPos);
        value = delegateCompressedValue.get(internalPos);
      }

      for (int i = 0; i < count; i++)
        res[resPos++] = value;
    }
    return res;
  }

  @Override
  public long get(int index) throws ArrayIndexOutOfBoundsException {
    int internalSize;
    if (compressedCounts != null)
      internalSize = compressedCounts.length;
    else
      internalSize = delegateCompressedCounts.size();

    if (index < 0 || index >= size())
      throw new ArrayIndexOutOfBoundsException("Index out of bounds");

    if (index == 0) {
      if (compressedValues != null)
        return compressedValues[0];
      return delegateCompressedValue.get(0);
    }

    int decompressedCount = 0;
    for (int pos = 0; pos < internalSize - 1; pos++) {

      long lengthValue;
      if (compressedValues != null)
        lengthValue = compressedCounts[pos];
      else
        lengthValue = delegateCompressedCounts.get(pos);

      decompressedCount += lengthValue;

      if (decompressedCount > index) {
        if (compressedValues != null)
          return compressedValues[pos];
        return delegateCompressedValue.get(pos);
      }
    }
    // should never happen
    if (compressedValues != null)
      return compressedValues[internalSize - 1];
    return delegateCompressedValue.get(internalSize - 1);
  }

}
