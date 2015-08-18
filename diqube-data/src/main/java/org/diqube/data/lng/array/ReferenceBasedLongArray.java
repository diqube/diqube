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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayReference;

/**
 * Compresses a long array by finding the avg value and then only storing the deltas to this avg value in another long
 * array.
 * 
 * <p>
 * As this compression itself is not as useful (compressed array is same size as input array), this class implements
 * {@link TransitiveExplorableCompressedLongArray} allowing users to stack another {@link ExplorableCompressedLongArray}
 * into this class in order to store the delta array. As that value-space of the deltas might be drastically smaller
 * than the one of the input array, it might be meaningful to provide a {@link BitEfficientLongArray} as inner
 * compression.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SLongCompressedArrayReference.class)
public class ReferenceBasedLongArray
    extends AbstractTransitiveExplorableCompressedLongArray<SLongCompressedArrayReference> {

  private boolean isSorted;
  private boolean isSameValue;
  private long refPoint;
  private long min;
  private long secondMin;
  private long max;
  /**
   * The compressed values, if they are stored here directly (= {@link #delegateCompressedValueLongArray} == null).
   * Otherwise, this field is <code>null</code>.
   */
  private long[] compressedValues;
  /**
   * The compressed values as stored in another {@link CompressedLongArray}. This is <code>null</code> if the compressed
   * values are stored in {@link #compressedValues}.
   */
  private ExplorableCompressedLongArray<?> delegateCompressedValueLongArray = null;

  public ReferenceBasedLongArray(long[] inputArray, boolean isSorted) {
    super();
    compress(inputArray, isSorted);
  }

  public ReferenceBasedLongArray() {
    super();
  }

  @Override
  protected double doExpectedCompressionRatio(long[] inputArray, boolean isSorted) {
    if (inputArray.length == 0)
      return 1.;
    return 1.;
  }

  @Override
  protected double doTransitiveExpectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator) {
    if (inputArray.length == 0)
      return 1.;

    return transitiveCalculator.calculateTransitiveCompressionRatio(min - refPoint, secondMin - refPoint,
        max - refPoint, inputArray.length);
  }

  @Override
  protected void doPrepareCompression(long[] inputArray, boolean isSorted) {
    this.isSorted = isSorted;

    if (inputArray.length == 0) {
      isSameValue = true;
      return;
    }

    if (isSorted) {
      min = inputArray[0];
      max = inputArray[inputArray.length - 1];
    } else {
      min = Long.MAX_VALUE;
      secondMin = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      for (int i = 0; i < inputArray.length; i++) {
        if (inputArray[i] < min) {
          if (min < secondMin)
            secondMin = min;
          min = inputArray[i];
        }
        if (inputArray[i] < secondMin && inputArray[i] > min)
          secondMin = inputArray[i];
        if (inputArray[i] > max)
          max = inputArray[i];
      }
    }

    isSameValue = min == max;

    if (min < 0 && max <= 0) {
      if (min - max >= Long.MIN_VALUE + 1)
        // we know that min..max is at max Long.MAX_VALUE, so we force refPoint in a way that the compressed array will
        // only hold positive values (this is good when using BitEfficientLongArray, as negative values force that to
        // use a sign bit).
        refPoint = min;
      else
        refPoint = min - Math.abs(Math.floorDiv(max, 2)) + Math.abs(Math.floorDiv(min, 2));
    } else if (min >= 0 && max > 0) {
      // again: deltas are in range 0..Long.MAX_VALUE, use only positive values.
      refPoint = min;
    } else if (min == max && max == 0) {
      refPoint = 0;
    } else {
      // min < 0 && max > 0
      long minAbs = (min == Long.MIN_VALUE) ? Long.MAX_VALUE : Math.abs(min);
      refPoint = min + Math.floorDiv(minAbs, 2) + Math.floorDiv(max, 2);
      // TODO think of calculating refPoint in an overflow area and switch the compression calculation from - to + etc.
    }
  }

  @Override
  protected void doCompress(long[] inputArray, boolean isSorted) {
    if (refPoint == 0) {
      compressedValues = inputArray;
      return;
    }

    compressedValues = new long[inputArray.length];

    // compress
    for (int i = 0; i < inputArray.length; i++) {
      compressedValues[i] = inputArray[i] - refPoint;
    }
  }

  @Override
  public void compress(long[] inputArray, boolean isSorted,
      Supplier<ExplorableCompressedLongArray<?>> transitiveSupplier) throws IllegalStateException {
    compress(inputArray, isSorted);
    delegateCompressedValueLongArray = transitiveSupplier.get();
    delegateCompressedValueLongArray.compress(compressedValues, isSorted);
    compressedValues = null;
  }

  @Override
  public boolean isSameValue() {
    return isSameValue;
  }

  @Override
  public int size() {
    if (compressedValues != null)
      return compressedValues.length;
    return delegateCompressedValueLongArray.size();
  }

  @Override
  public boolean isSorted() {
    return isSorted;
  }

  @Override
  public long[] decompressedArray() {
    if (compressedValues != null) {
      long[] res = new long[compressedValues.length];
      for (int i = 0; i < res.length; i++)
        res[i] = compressedValues[i] + refPoint;
      return res;
    }

    long[] delegateResults = delegateCompressedValueLongArray.decompressedArray();
    for (int i = 0; i < delegateResults.length; i++)
      delegateResults[i] += refPoint;
    return delegateResults;
  }

  @Override
  public long get(int index) throws ArrayIndexOutOfBoundsException {
    if (compressedValues != null)
      return compressedValues[index] + refPoint;
    return delegateCompressedValueLongArray.get(index) + refPoint;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SLongCompressedArrayReference target)
      throws SerializationException {
    target.setIsSorted(isSorted);
    target.setIsSameValue(isSameValue);
    target.setRefPoint(refPoint);
    target.setMin(min);
    target.setSecondMin(secondMin);
    target.setMax(max);

    if (compressedValues != null) {
      target.setCompressedValues(LongStream.of(compressedValues).boxed().collect(Collectors.toList()));
    } else {
      target.setDelegateCompressedValues(
          mgr.serializeChild(SLongCompressedArray.class, delegateCompressedValueLongArray));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SLongCompressedArrayReference source)
      throws DeserializationException {
    isSorted = source.isIsSorted();
    isSameValue = source.isIsSameValue();
    refPoint = source.getRefPoint();
    min = source.getMin();
    secondMin = source.getSecondMin();
    max = source.getMax();
    if (source.isSetCompressedValues()) {
      compressedValues = source.getCompressedValues().stream().mapToLong(Long::longValue).toArray();
      delegateCompressedValueLongArray = null;
    } else {
      delegateCompressedValueLongArray =
          mgr.deserializeChild(ExplorableCompressedLongArray.class, source.getDelegateCompressedValues());
      compressedValues = null;
    }
  }

}
