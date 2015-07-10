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

/**
 * A compressed representation of a long[].
 *
 * @author Bastian Gloeckle
 */
public interface CompressedLongArray {
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
   * @return Decompressed long at given index. This runs in O(1).
   * @throws ArrayIndexOutOfBoundsException
   *           If index is invalid.
   */
  public long get(int index) throws ArrayIndexOutOfBoundsException;
}
