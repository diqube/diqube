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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayRLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

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
@DataSerializable(thriftClass = SLongCompressedArrayRLE.class)
public class RunLengthLongArray extends AbstractTransitiveExplorableCompressedLongArray<SLongCompressedArrayRLE> {
  private static final Logger logger = LoggerFactory.getLogger(RunLengthLongArray.class);

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
  /**
   * Contains compressed counts. If <code>null</code>, the compressed values are in {@link #delegateCompressedCounts}
   */
  private long[] compressedCounts;
  /** Contains compressed values. If <code>null</code>, the compressed values are in {@link #compressedValues} */
  private ExplorableCompressedLongArray<?> delegateCompressedValue = null;
  /** Contains compressed counts. If <code>null</code>, the compressed values are in {@link #compressedCounts} */
  private ExplorableCompressedLongArray<?> delegateCompressedCounts = null;
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
        if (lastCount > maxCount)
          maxCount = lastCount;
        if (lastCount < minCount)
          minCount = lastCount;

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
      if (lastValue < secondMinValue && lastValue > minValue)
        secondMinValue = lastValue;
    }

    if (lastCount > maxCount)
      maxCount = lastCount;
    if (lastCount < minCount)
      minCount = lastCount;

    if (secondMinValue > maxValue)
      secondMinValue = maxValue;
  }

  @Override
  protected double doTransitiveExpectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator) {
    double valueCompression = transitiveCalculator.calculateTransitiveCompressionRatio(minValue, secondMinValue,
        maxValue, numberOfDifferentTuples);
    double countCompression =
        transitiveCalculator.calculateTransitiveCompressionRatio(minCount, minCount, maxCount, numberOfDifferentTuples);

    double resRatio = (numberOfDifferentTuples * (valueCompression + countCompression)) / size;

    logger.trace(
        "Res ratio: {}, Value ratio: {}, count ratio: {}, minValue: {}, secondMinValue: {}, "
            + "maxValue: {}, minCount: {}, maxCount: {}, numberOfDifferentTuples:  {}",
        resRatio, valueCompression, countCompression, minValue, secondMinValue, maxValue, minCount, maxCount,
        numberOfDifferentTuples);

    return resRatio;
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
  public void compress(long[] inputArray, boolean isSorted,
      Supplier<ExplorableCompressedLongArray<?>> transitiveSupplier) throws IllegalStateException {
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

  @Override
  public List<Long> getMultiple(List<Integer> sortedIndices) throws ArrayIndexOutOfBoundsException {
    // first: Find the internal indices that we need to resolve
    List<Integer> internalIndicesToResolveSorted = new ArrayList<>();

    try {
      int internalSize;
      if (compressedCounts != null)
        internalSize = compressedCounts.length;
      else
        internalSize = delegateCompressedCounts.size();

      PeekingIterator<Integer> sortedIndicesIt = Iterators.peekingIterator(sortedIndices.iterator());
      if (sortedIndicesIt.peek() < 0 || sortedIndicesIt.peek() >= size)
        throw new ArrayIndexOutOfBoundsException("Array index out of bounds: Requested index " + sortedIndicesIt.peek()
            + " but have only " + size + " elements.");

      int decompressedCount = 0;
      for (int pos = 0; pos < internalSize && sortedIndicesIt.hasNext(); pos++) {
        long lengthValue;
        if (compressedValues != null)
          lengthValue = compressedCounts[pos];
        else
          lengthValue = delegateCompressedCounts.get(pos);

        decompressedCount += lengthValue;

        while (sortedIndicesIt.hasNext() && sortedIndicesIt.peek() < decompressedCount) {
          internalIndicesToResolveSorted.add(pos);
          sortedIndicesIt.next();
          if (sortedIndicesIt.hasNext() && (sortedIndicesIt.peek() < 0 || sortedIndicesIt.peek() >= size))
            throw new ArrayIndexOutOfBoundsException("Array index out of bounds: Requested index "
                + sortedIndicesIt.peek() + " but have only " + size + " elements.");
        }
      }
    } catch (Throwable t) {
      throw t;
    }

    // second: resolve those internal indices
    List<Long> res = new ArrayList<>();
    if (compressedValues != null) {
      for (int idx : internalIndicesToResolveSorted)
        res.add(compressedValues[idx]);
    } else {
      // unique-ify indices to resolve
      List<Integer> delegateIdx = new ArrayList<>(new TreeSet<>(internalIndicesToResolveSorted));
      List<Long> delegateRes = delegateCompressedValue.getMultiple(delegateIdx);

      PeekingIterator<Integer> delegateIdxIt = Iterators.peekingIterator(delegateIdx.iterator());
      PeekingIterator<Long> delegateResIt = Iterators.peekingIterator(delegateRes.iterator());

      for (int idx : internalIndicesToResolveSorted) {
        while (delegateIdxIt.peek() != idx) {
          delegateIdxIt.next();
          delegateResIt.next();
        }

        res.add(delegateResIt.peek());
      }
    }
    return res;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SLongCompressedArrayRLE target) throws SerializationException {
    target.setSize(size);
    target.setIsSorted(isSorted);
    target.setNumberOfDifferentTuples(numberOfDifferentTuples);
    target.setMaxValue(maxValue);
    target.setMaxCount(maxCount);
    target.setMinValue(minValue);
    target.setSecondMinValue(secondMinValue);
    target.setMinCount(minCount);
    if (compressedValues != null) {
      target.setCompressedValues(LongStream.of(compressedValues).boxed().collect(Collectors.toList()));
      target.setCompressedCounts(LongStream.of(compressedCounts).boxed().collect(Collectors.toList()));
    } else {
      target.setDelegateCompressedValue(mgr.serializeChild(SLongCompressedArray.class, delegateCompressedValue));
      target.setDelegateCompressedCounts(mgr.serializeChild(SLongCompressedArray.class, delegateCompressedCounts));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SLongCompressedArrayRLE source) throws DeserializationException {
    size = source.getSize();
    isSorted = source.isIsSorted();
    numberOfDifferentTuples = source.getNumberOfDifferentTuples();
    maxValue = source.getMaxValue();
    maxCount = source.getMaxCount();
    minValue = source.getMinValue();
    secondMinValue = source.getSecondMinValue();
    minCount = source.getMinCount();
    if (source.isSetCompressedValues()) {
      compressedValues = source.getCompressedValues().stream().mapToLong(Long::longValue).toArray();
      compressedCounts = source.getCompressedCounts().stream().mapToLong(Long::longValue).toArray();
      delegateCompressedValue = null;
      delegateCompressedCounts = null;
    } else {
      delegateCompressedValue =
          mgr.deserializeChild(ExplorableCompressedLongArray.class, source.getDelegateCompressedValue());
      delegateCompressedCounts =
          mgr.deserializeChild(ExplorableCompressedLongArray.class, source.getDelegateCompressedCounts());
      compressedValues = null;
      compressedCounts = null;
    }
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this
        53 + // small fields
        ((compressedCounts != null) ? compressedCounts.length * 8 : 0)
        + ((compressedValues != null) ? compressedValues.length * 8 : 0)
        + ((delegateCompressedCounts != null) ? delegateCompressedCounts.calculateApproximateSizeInBytes() : 0)
        + ((delegateCompressedValue != null) ? delegateCompressedValue.calculateApproximateSizeInBytes() : 0);
  }

}
