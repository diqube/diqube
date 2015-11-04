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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.array.CompressedLongArrayUtil;

/**
 * A {@link CompressedLongArray} whose values are based on a delegate, but the values at specific indices of the
 * delegate array are not available through this facade.
 *
 * <p>
 * This class optionally adds a delta to all values it returns.
 * 
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class IndexRemovingCompressedLongArray implements CompressedLongArray<TBase<?, ?>> {
  private CompressedLongArray<?> delegate;
  private CompressedLongArray<?> sortedRemoveIndices;
  private boolean isSameValue;
  private long valueDelta;

  /**
   * @param sortedRemoveIndices
   *          sorted list of indices that should be "removed" from the delegate. Be aware that this array should be
   *          accessible in constant-time, i.e. this should not be a run-length encoded one.
   */
  /* package */ IndexRemovingCompressedLongArray(CompressedLongArray<?> delegate,
      CompressedLongArray<?> sortedRemoveIndices, long valueDelta) {
    this.delegate = delegate;
    this.sortedRemoveIndices = sortedRemoveIndices;
    this.valueDelta = valueDelta;

    isSameValue = LongStream.of(decompressedArray()).distinct().count() == 1;
  }

  @Override
  public boolean isSameValue() {
    return isSameValue;
  }

  @Override
  public int size() {
    return delegate.size() - sortedRemoveIndices.size();
  }

  @Override
  public boolean isSorted() {
    return delegate.isSorted();
  }

  @Override
  public long[] decompressedArray() {
    long[] delResult = delegate.decompressedArray();
    if (sortedRemoveIndices.size() == 0)
      return delResult;

    long[] res = new long[delResult.length - sortedRemoveIndices.size()];

    int resPos = 0;
    int nextRemoveIndicesPos = 0;
    for (int i = 0; i < delResult.length; i++) {
      if (nextRemoveIndicesPos == sortedRemoveIndices.size()) {
        res[resPos++] = delResult[i] + valueDelta;
      } else {
        if (sortedRemoveIndices.get(nextRemoveIndicesPos) == i) {
          nextRemoveIndicesPos++;
          continue;
        }

        res[resPos++] = delResult[i] + valueDelta;
      }
    }

    return res;
  }

  @Override
  public long get(int index) throws ArrayIndexOutOfBoundsException {
    if (index < 0 || index >= size())
      throw new ArrayIndexOutOfBoundsException(
          "Tried to acces index " + index + " but there are " + size() + " entries.");

    return delegate.get(toDelegateIndices(Arrays.asList(index)).get(0)) + valueDelta;
  }

  @Override
  public List<Long> getMultiple(List<Integer> sortedIndices) throws ArrayIndexOutOfBoundsException {
    if (sortedIndices.stream().anyMatch(i -> i < 0 || i >= size()))
      throw new ArrayIndexOutOfBoundsException(
          "Tried to acces indexes " + sortedIndices + " but there are " + size() + " entries.");

    List<Long> delRes = delegate.getMultiple(toDelegateIndices(sortedIndices));

    if (valueDelta != 0)
      delRes = delRes.stream().map(v -> v + valueDelta).collect(Collectors.toList());

    return delRes;
  }

  private List<Integer> toDelegateIndices(List<Integer> inputIndices) {
    List<Integer> res = new ArrayList<>();
    for (int inputIdx : inputIndices) {
      int curRemovedIdx = CompressedLongArrayUtil.binarySearch(sortedRemoveIndices, inputIdx);
      int numberOfRemovedEntriesBefore = curRemovedIdx;
      if (numberOfRemovedEntriesBefore < 0) {
        int insertionPoint = -1 - numberOfRemovedEntriesBefore;
        numberOfRemovedEntriesBefore = insertionPoint;
        curRemovedIdx = insertionPoint;
      }

      // numberOfRemovedEntriesBefore number of entries are removed in the input indices. We therefore have to advance
      // the inputIdx by that many not-again-removed entries to get the delegate idx.

      while (numberOfRemovedEntriesBefore > 0) {
        inputIdx++;
        while (sortedRemoveIndices.get(curRemovedIdx) < inputIdx)
          curRemovedIdx++;
        if (sortedRemoveIndices.get(curRemovedIdx) != inputIdx)
          // index is not again removed
          numberOfRemovedEntriesBefore--;
      }
      res.add(inputIdx);
    }
    return res;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    // do not include size of delegate.
    return 16 + // object header of this
        sortedRemoveIndices.calculateApproximateSizeInBytes() + //
        10;
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, TBase<?, ?> target)
      throws SerializationException {
    throw new SerializationException("Flattened data cannot be serialized");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      TBase<?, ?> source) throws DeserializationException {
    throw new DeserializationException("Flattened data cannot be deserialized");
  }
}
