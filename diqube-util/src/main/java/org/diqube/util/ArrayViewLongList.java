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
package org.diqube.util;

import java.util.AbstractList;
import java.util.List;

/**
 * A {@link List} of Long that is unmodifiable and returns the values of a specific range of a base array.
 *
 * @author Bastian Gloeckle
 */
public class ArrayViewLongList extends AbstractList<Long> {

  private Long[] baseArray;
  private int start;
  private int length;

  public ArrayViewLongList(Long[] baseArray, int start, int length) {
    this.baseArray = baseArray;
    this.start = start;
    this.length = length;
  }

  @Override
  public Long get(int index) {
    return baseArray[index + start];
  }

  @Override
  public int size() {
    return length;
  }

}
