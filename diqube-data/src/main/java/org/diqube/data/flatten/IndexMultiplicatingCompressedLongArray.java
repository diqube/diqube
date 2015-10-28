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
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.types.lng.array.CompressedLongArray;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A {@link CompressedLongArray} whose values are based on a delegate, but the values at specific indices of the
 * delegate array are returned for multiple consecutive indices by this facade.
 * 
 * <p>
 * This class optionally adds a delta to all values it returns.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class IndexMultiplicatingCompressedLongArray implements CompressedLongArray<TBase<?, ?>> {
  private CompressedLongArray<?> delegate;
  /** Intersting input idx -> delegate index. Only contains those input indices for which the delegate index changes. */
  private NavigableMap<Integer, Integer> inputToDelegateIndex;
  private int size;
  private long valueDelta;

  /* package */ IndexMultiplicatingCompressedLongArray(CompressedLongArray<?> delegate,
      Map<Integer, Integer> multiplicationFactors, long valueDelta) {
    this.delegate = delegate;
    this.valueDelta = valueDelta;

    inputToDelegateIndex = new TreeMap<>();
    int nextInputIdx = 0;
    for (int delegateIdx = 0; delegateIdx < delegate.size(); delegateIdx++) {
      int mul = (multiplicationFactors.containsKey(delegateIdx)) ? multiplicationFactors.get(delegateIdx) : 1;
      inputToDelegateIndex.put(nextInputIdx, delegateIdx);

      nextInputIdx += mul;
    }
    size = nextInputIdx;
  }

  @Override
  public boolean isSameValue() {
    return delegate.isSameValue();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isSorted() {
    return delegate.isSorted();
  }

  @Override
  public long[] decompressedArray() {
    long[] delResult = delegate.decompressedArray();

    long[] res = new long[size];

    int delIdx = 0;
    int idx = 0;
    PeekingIterator<Integer> interestingIndexIt = Iterators.peekingIterator(inputToDelegateIndex.keySet().iterator());
    interestingIndexIt.next();
    while (idx < size) {
      int upToIdx = (interestingIndexIt.hasNext()) ? interestingIndexIt.peek() : size;
      while (idx < upToIdx)
        res[idx++] = delResult[delIdx] + valueDelta;

      delIdx++;
      interestingIndexIt.next();
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

  private List<Integer> toDelegateIndices(List<Integer> inputIndices) throws ArrayIndexOutOfBoundsException {
    List<Integer> res = new ArrayList<>();
    for (int inputIdx : inputIndices) {
      Entry<Integer, Integer> e = inputToDelegateIndex.floorEntry(inputIdx);
      if (e == null)
        throw new ArrayIndexOutOfBoundsException("Found no delegate index for input idx " + inputIdx);

      res.add(e.getValue());
    }
    return res;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    // do not include size of delegate.
    return 16 + // object header of this
        inputToDelegateIndex.size() * 8;
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
