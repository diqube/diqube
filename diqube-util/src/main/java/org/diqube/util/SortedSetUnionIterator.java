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

import java.util.Iterator;
import java.util.SortedSet;
import java.util.stream.Stream;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A {@link Iterator} that combines the iterators of multiple {@link SortedSet}s and returns an iterator that returns
 * all elements of those sets in sorted order.
 *
 * @author Bastian Gloeckle
 */
public class SortedSetUnionIterator<E extends Comparable<E>> implements Iterator<E> {
  private PeekingIterator<E>[] iterators;

  @SuppressWarnings("unchecked")
  public SortedSetUnionIterator(SortedSet<E>... sets) {
    iterators = new PeekingIterator[sets.length];
    for (int i = 0; i < sets.length; i++)
      iterators[i] = Iterators.peekingIterator(sets[i].iterator());
  }

  @Override
  public boolean hasNext() {
    return Stream.of(iterators).anyMatch(i -> i.hasNext());
  }

  @Override
  public E next() {
    if (!hasNext())
      return null;
    PeekingIterator<E> nextSmallestIt = null;
    for (PeekingIterator<E> it : iterators) {
      if (!it.hasNext())
        continue;

      if (nextSmallestIt == null || (nextSmallestIt.peek().compareTo(it.peek()) > 0))
        nextSmallestIt = it;
    }
    return nextSmallestIt.next();
  }
}
