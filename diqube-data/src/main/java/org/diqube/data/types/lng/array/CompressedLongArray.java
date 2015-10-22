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

import java.util.List;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;

/**
 * A compressed representation of a long[].
 *
 * @param <T>
 *          Thrift class this long array can be serialized to/from.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SLongCompressedArray.class,
    deserializationDelegationManager = CompressedLongArrayDeserializationDelegationManager.class)
public interface CompressedLongArray<T extends TBase<?, ?>> extends DataSerialization<T> {
  /**
   * @return True if the decompressed values are equal for every index in the array.
   */
  public boolean isSameValue();

  /**
   * @return Number of items stored.
   */
  public int size();

  /**
   * @return True if the decompressed values are sorted.
   */
  public boolean isSorted();

  /**
   * @return Decompressed Array. This runs in O(n).
   */
  public long[] decompressedArray();

  /**
   * @return Decompressed long at given index.
   * @throws ArrayIndexOutOfBoundsException
   *           If index is invalid.
   */
  public long get(int index) throws ArrayIndexOutOfBoundsException;

  /**
   * Returns multiple entries of this array.
   * 
   * This method should be preferred compared to {@link #get(int)}, as the compression might be able to work more
   * effectively if it can resolve multiple values at once.
   * 
   * @param sortedIndices
   *          Sorted!
   * @return Decompressed long at given index.
   * @throws ArrayIndexOutOfBoundsException
   *           If any index is invalid.
   */
  public List<Long> getMultiple(List<Integer> sortedIndices) throws ArrayIndexOutOfBoundsException;

  /**
   * @return An approximate number of bytes taken up by this {@link CompressedLongArray}. Note that this is only an
   *         approximation!
   */
  public long calculateApproximateSizeInBytes();
}
