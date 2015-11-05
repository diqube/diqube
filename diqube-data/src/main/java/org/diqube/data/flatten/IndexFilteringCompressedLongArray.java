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

/**
 * A {@link CompressedLongArray} whose values are based on a delegate, but only the values at specific indices of the
 * delegate array are available through this facade.
 *
 * <p>
 * This class optionally adds a delta to all values it returns.
 * 
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class IndexFilteringCompressedLongArray implements CompressedLongArray<TBase<?, ?>> {
  private CompressedLongArray<?> delegate;
  private CompressedLongArray<?> sortedFilteredIndices;
  private volatile Boolean isSameValue;
  private long valueDelta;

  /**
   * @param sortedFilteredIndices
   *          sorted list of delegate-indices that should be available through this IndexFilteringCompressedLongArray.
   *          Be aware that this array should be accessible in constant-time, i.e. this should not be a run-length
   *          encoded one.
   */
  /* package */ IndexFilteringCompressedLongArray(CompressedLongArray<?> delegate,
      CompressedLongArray<?> sortedFilteredIndices, long valueDelta) {
    this.delegate = delegate;
    this.sortedFilteredIndices = sortedFilteredIndices;
    this.valueDelta = valueDelta;

    // initialize lazily, as this takes some time.
    isSameValue = null;
  }

  @Override
  public boolean isSameValue() {
    if (isSameValue == null) {
      synchronized (this) {
        if (isSameValue == null)
          isSameValue = LongStream.of(decompressedArray()).distinct().count() == 1;
      }
    }
    return isSameValue;
  }

  @Override
  public int size() {
    return sortedFilteredIndices.size();
  }

  @Override
  public boolean isSorted() {
    return delegate.isSorted();
  }

  @Override
  public long[] decompressedArray() {
    if (sortedFilteredIndices.size() == 0)
      return new long[0];

    List<Long> res = delegate.getMultiple(
        LongStream.of(sortedFilteredIndices.decompressedArray()).mapToObj(l -> (int) l).collect(Collectors.toList()));

    return res.stream().mapToLong(l -> l.longValue() + valueDelta).toArray();
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
    for (int inputIdx : inputIndices)
      res.add((int) sortedFilteredIndices.get(inputIdx));
    return res;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    // do not include size of delegate.
    return 16 + // object header of this
        sortedFilteredIndices.calculateApproximateSizeInBytes() + //
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
