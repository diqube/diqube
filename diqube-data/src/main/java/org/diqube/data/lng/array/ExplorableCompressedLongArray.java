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

import org.apache.thrift.TBase;

/**
 * Instances can be used to not directly compress an input array, but retrieve the compression ratio beforehand.
 * 
 * <p>
 * This enables callers to make an informed decision if a specific compression should be used or not.
 * 
 * <p>
 * An {@link ExplorableCompressedLongArray} is either in the {@link State#EXPLORING} or {@link State#COMPRESSED}. While
 * in exploring state, the method {@link #expectedCompressionRatio(long[], boolean)} can be called. The transition from
 * exploring to compressed is made by calling the {@link #compress(long[], boolean)} method and compressing actual
 * values. There is not possibility to go back to exploring state. Implementations may choose to enter compressed state
 * right away using specific constructors, the default constructor should instantiate the object in exploring state.
 *
 * <p>
 * Methods of {@link CompressedLongArray} implemented can only be called when in compressed state.
 *
 * @param <T>
 *          Thrift class this long array can be serialized to/from.
 * 
 * @author Bastian Gloeckle
 */
public interface ExplorableCompressedLongArray<T extends TBase<?, ?>> extends CompressedLongArray<T> {
  public enum State {
    EXPLORING, COMPRESSED
  }

  /**
   * Calculate an approximation of the compression ratio the compression would achieve on the given input array.
   * 
   * <p>
   * This call caches intermediate calculation results needed to calculate the compression ratio and if the
   * {@link #compress(long[], boolean)} method is called later on with the same input array, these cached results will
   * be used to actually compress the array.
   * 
   * <p>
   * Only callable in {@link State#EXPLORING}.
   * 
   * @param inputArray
   *          The array of uncompressed values whose approximate compression ratio should be calculated.
   * @param isSorted
   *          If the inputArray is sorted.
   * @return Approximate ratio of how much memory would be used when compressing the input array in comparison to the
   *         memory used by the uncompressed array. This value will be > 0.
   * @throws IllegalStateException
   *           if in wrong {@link State}.
   */
  public double expectedCompressionRatio(long[] inputArray, boolean isSorted) throws IllegalStateException;

  /**
   * Compress the given array.
   * 
   * If the same array was used in the last call to {@link #expectedCompressionRatio(long[], boolean)} then the
   * intermediary results of that call will be used.
   * 
   * @param inputArray
   *          The array that should be compressed.
   * @param isSorted
   *          If the input array is sorted.
   * @throws IllegalStateException
   *           If there is a value compressed already.
   */
  public void compress(long[] inputArray, boolean isSorted) throws IllegalStateException;

}
