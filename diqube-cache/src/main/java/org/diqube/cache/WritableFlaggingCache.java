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

/**
 * A {@link FlaggingCache} that can be written to.
 *
 * @author Bastian Gloeckle
 */
public interface WritableFlaggingCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V>
    extends FlaggingCache<K1, K2, V>, WritableCache<K1, K2, V> {
  /**
   * Offer an element to the cache and flag it right away.
   * 
   * <p>
   * This will either lead to flagging the currently available entry in the cache for that (K1,K2), or to adding the new
   * value and flagging it. After calling this method, there will be an entry in the cache for the given (K1, K2).
   * 
   * @see #flagAndGet(Comparable, Comparable, long)
   * @param key1
   *          key part one.
   * @param key2
   *          key part two.
   * @param value
   *          The value to be cached.
   * @param flagUntilNanos
   *          {@link System#nanoTime()} of when the flag should be removed from the value earliest.
   * @return The value instance now stored in the cache.
   */
  public V offerAndFlag(K1 key1, K2 key2, V value, long flagUntilNanos);
}
