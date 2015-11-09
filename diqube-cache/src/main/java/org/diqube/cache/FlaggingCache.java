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
 * Cache that has the capability to flag specific entries for them to not be evicted from the cache for a certain amount
 * of time.
 *
 * @author Bastian Gloeckle
 */
public interface FlaggingCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V> extends Cache<K1, K2, V> {
  /**
   * Gets and flags a cache entry.
   * 
   * <p>
   * Flagging an entry means that it will not be evicted from the cache at least until {@link System#nanoTime()} is
   * equal to flagUntilNanos. It is not guaranteed though that the element will be evicted soon after that time (if it
   * would be up for eviction because of other circumstances).
   * 
   * <p>
   * Only valid cache entries will be flagged, apparently. That means that if this method returns <code>null</code>,
   * then nothing is flagged.
   * 
   * @param key1
   *          Part one of the key.
   * @param key2
   *          Part two of the key.
   * @param flagUntilNanos
   *          {@link System#nanoTime()} at which the flag should be removed the earliest and therefore enable evicting
   *          the value again.
   * @return The cached element or <code>null</code>.
   */
  public V flagAndGet(K1 key1, K2 key2, long flagUntilNanos);

}
