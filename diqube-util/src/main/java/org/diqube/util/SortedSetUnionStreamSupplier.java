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

import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Provides a Stream that is built from an {@link SortedSetUnionIterator}.
 * 
 * <p>
 * Each call to the {@link Supplier#get()} provides a new instance.
 *
 * @author Bastian Gloeckle
 */
public class SortedSetUnionStreamSupplier<E extends Comparable<E>> implements Supplier<Stream<E>> {

  private SortedSet<E>[] sets;

  @SafeVarargs
  public SortedSetUnionStreamSupplier(SortedSet<E>... sets) {
    this.sets = sets;
  }

  @Override
  public Stream<E> get() {
    SortedSetUnionIterator<E> it = new SortedSetUnionIterator<>(sets);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED | Spliterator.DISTINCT),
        false);
  }

}
