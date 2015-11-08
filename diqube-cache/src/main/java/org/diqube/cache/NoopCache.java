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
package org.diqube.cache;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Cache doing nothing.
 *
 * @author Bastian Gloeckle
 */
public class NoopCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V> implements WritableCache<K1, K2, V> {

  @Override
  public V get(K1 key1, K2 key2) {
    return null;
  }

  @Override
  public Collection<V> getAll(K1 key1) {
    return new ArrayList<>();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean offer(K1 key1, K2 key2, V value) {
    return false;
  }
}