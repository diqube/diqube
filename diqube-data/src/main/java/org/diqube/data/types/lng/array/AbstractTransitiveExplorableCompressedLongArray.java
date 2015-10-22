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

import org.apache.thrift.TBase;

/**
 * Abstract base implementation for {@link TransitiveExplorableCompressedLongArray}.
 *
 * @param <T>
 *          Thrift class this long array can be serialized to/from.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractTransitiveExplorableCompressedLongArray<T extends TBase<?, ?>>
    extends AbstractExplorableCompressedLongArray<T>implements TransitiveExplorableCompressedLongArray<T> {

  @Override
  public double expectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator) throws IllegalStateException {
    if (!State.EXPLORING.equals(state))
      throw new IllegalStateException("Not in Exploring state.");
    prepareCompression(inputArray, isSorted);
    return doTransitiveExpectedCompressionRatio(inputArray, isSorted, transitiveCalculator);
  }

  /**
   * @see TransitiveExplorableCompressedLongArray#expectedCompressionRatio(long[], boolean,
   *      org.diqube.data.types.lng.array.ExplorableCompressedLongArray.TransitiveCompressionRatioCalculator)
   */
  abstract protected double doTransitiveExpectedCompressionRatio(long[] inputArray, boolean isSorted,
      TransitiveCompressionRatioCalculator transitiveCalculator);
}
