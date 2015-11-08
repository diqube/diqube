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

import java.util.Collection;

/**
 * Caches elements of type V which are keyed by pairs of (K1, K2).
 *
 *
 * @param <K1>
 *          Type of key part 1.
 * @param <K2>
 *          Type of key part 2.
 * @param <V>
 *          Value type.
 * @author Bastian Gloeckle
 */
public interface Cache<K1, K2, V> {
  /**
   * Get a specific cached element for the given key pair.
   * 
   * @param key1
   *          part one of the key.
   * @param key2
   *          part two of the key.
   * @return The cached value or <code>null</code> if not available.
   */
  public V get(K1 key1, K2 key2);

  /**
   * Get all cached elements for the given first part of key pairs.
   * 
   * @param key1
   *          first part of the key.
   * @return All cached elements. May be empty.
   */
  public Collection<V> getAll(K1 key1);

  /**
   * @return Number of cached elements.
   */
  public int size();
}
