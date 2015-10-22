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

import java.lang.ref.WeakReference;

import org.apache.thrift.TBase;

/**
 * Abstract implementation of {@link ExplorableCompressedLongArray} which handles the {@link State} transitions and
 * introduces compression preparation.
 *
 * @param <T>
 *          Thrift class this long array can be serialized to/from.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractExplorableCompressedLongArray<T extends TBase<?, ?>>
    implements ExplorableCompressedLongArray<T> {
  /** {@link State} this instance is in. */
  protected State state;
  /**
   * If not <code>null</code> this is a reference to the array that was {@link #prepareCompression(long[], boolean)}d
   * last (i.e. that was used in a call to {@link #expectedCompressionRatio(long[], boolean)} last).
   */
  protected WeakReference<long[]> preparedInputArray = null;

  public AbstractExplorableCompressedLongArray() {
    state = State.EXPLORING;
  }

  @Override
  public double expectedCompressionRatio(long[] inputArray, boolean isSorted) throws IllegalStateException {
    if (!State.EXPLORING.equals(state))
      throw new IllegalStateException("Not in Exploring state.");
    prepareCompression(inputArray, isSorted);
    return doExpectedCompressionRatio(inputArray, isSorted);
  }

  /**
   * Calculate an approximation of the compression ratio of the given input array.
   * 
   * This method can safely assume that {@link #doPrepareCompression(long[], boolean)} was called before with the same
   * array and the fields of the object that are set by latter method are still in the correct state.
   * 
   * @param inputArray
   *          See {@link #expectedCompressionRatio(long[], boolean)}
   * @param isSorted
   *          See {@link #expectedCompressionRatio(long[], boolean)}
   * @return See {@link #expectedCompressionRatio(long[], boolean)}
   */
  abstract protected double doExpectedCompressionRatio(long[] inputArray, boolean isSorted);

  @Override
  public void compress(long[] inputArray, boolean isSorted) throws IllegalStateException {
    if (!State.EXPLORING.equals(state))
      throw new IllegalStateException("Not in Exploring state.");
    prepareCompression(inputArray, isSorted);
    doCompress(inputArray, isSorted);
    state = State.COMPRESSED;
  }

  /**
   * Compress the input array.
   * 
   * This method can safely assume that {@link #doPrepareCompression(long[], boolean)} was called before with the same
   * array and the fields of the object that are set by latter method are still in the correct state.
   * 
   * @param inputArray
   *          The input array to compress.
   * @param isSorted
   *          true if the input array is sorted.
   */
  abstract protected void doCompress(long[] inputArray, boolean isSorted);

  protected void prepareCompression(long[] inputArray, boolean isSorted) {
    if (preparedInputArray != null && preparedInputArray.get() == inputArray)
      // we prepared the values for this input array already, no need to do it again.
      return;
    doPrepareCompression(inputArray, isSorted);
    preparedInputArray = new WeakReference<long[]>(inputArray);
  }

  /**
   * Prepares the compression by evaluating the input array and calculating any compression-specific statistics from it.
   * 
   * <p>
   * This method should prepare all data that is needed for either {@link #doCompress(long[], boolean)} or
   * {@link #doExpectedCompressionRatio(long[], boolean)} calculations. This method is guaranteed to be executed before
   * those calculation methods are called.
   * 
   * <p>
   * This method should not execute the compression itself.
   * 
   * <p>
   * It is expected that calling this method will have side-effects on the fields of the object.
   * 
   * @param inputArray
   *          The array of which the compression-statistics are calculated.
   * @param isSorted
   *          true if the input array is sorted.
   */
  abstract protected void doPrepareCompression(long[] inputArray, boolean isSorted);

  @Override
  public String toString() {
    // for debugging we implement the toString method.
    StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getSimpleName());
    sb.append("(");
    sb.append(this.size());
    sb.append(")[");
    for (int i = 0; i < Math.min(10, this.size()); i++) {
      if (i > 0)
        sb.append(",");
      sb.append(this.get(i));
    }
    if (this.size() >= 10)
      sb.append("...");
    sb.append("]");

    return sb.toString();
  }
}
