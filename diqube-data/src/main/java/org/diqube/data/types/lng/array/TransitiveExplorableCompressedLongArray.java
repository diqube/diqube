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

import java.util.function.Supplier;

import org.apache.thrift.TBase;

/**
 * A {@link ExplorableCompressedLongArray} that allows compression using another {@link ExplorableCompressedLongArray}
 * for the compression of the results of the outer compression.
 *
 * @param <T>
 *          Thrift class this long array can be serialized to/from.
 *
 * @author Bastian Gloeckle
 */
public interface TransitiveExplorableCompressedLongArray<T extends TBase<?, ?>>
    extends ExplorableCompressedLongArray<T> {
  /**
   * Like {@link #expectedCompressionRatio(long[], boolean)}, but expects that there will be another
   * {@link ExplorableCompressedLongArray} used as storage for this compressed array.
   * 
   * @param transitiveCalculator
   *          The calculator that can calculate the compression ratio of the transitive {@link CompressedLongArray}.
   */
  public double expectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator) throws IllegalStateException;

  /**
   * Like {@link #compress(long[], boolean)}, but expects that there will be another
   * {@link ExplorableCompressedLongArray} used as storage for this compressed array, of which an instance is supplied
   * by the Supplier.
   */
  public void compress(long[] inputArray, boolean isSorted,
      Supplier<ExplorableCompressedLongArray<?>> transitiveSupplier) throws IllegalStateException;

  /**
   * Calculates the compression ratio that would be available for a specific range of values.
   */
  public static interface TransitiveCompressionRatioCalculator {
    /**
     * @param min
     *          The minimal value that would be compressed.
     * @param secondMin
     *          The second-most minimal value that would be compressed.
     * @param max
     *          The max value that would be compressed.
     * @param size
     *          The size of the array to be compressed
     * @return Compression ratio when values with the given range are compressed (min and max both included in the
     *         range).
     */
    public double calculateTransitiveCompressionRatio(long min, long secondMin, long max, long size);
  }
}
