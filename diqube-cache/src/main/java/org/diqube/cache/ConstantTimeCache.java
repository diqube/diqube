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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.diqube.util.Pair;
import org.diqube.util.Triple;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * {@link Cache} that caches each entry for a specific amount of time and evicts it afterwards.
 * 
 * <p>
 * Calls to {@link #offer(Comparable, Comparable, Object)} will not accept a new object, if one with the same keys is
 * registered already. The timeout will not be updated in that case, either.
 * 
 * If no object with the same keys is registered, {@link #offer(Comparable, Comparable, Object)} will always accept new
 * values.
 *
 * @author Bastian Gloeckle
 */
public class ConstantTimeCache<K1 extends Comparable<K1>, K2 extends Comparable<K2>, V>
    implements WritableCache<K1, K2, V>, DeletableCache<K1, K2, V> {

  private long cacheTimeMs;

  private ConcurrentMap<Pair<K1, K2>, V> values = new ConcurrentHashMap<>();
  private ConcurrentSkipListSet<Triple<Long, K1, K2>> timeouts = new ConcurrentSkipListSet<>();
  private ConcurrentMap<K1, Set<K2>> secondLevelKeys = new ConcurrentHashMap<>();

  public ConstantTimeCache(long cacheTimeMs) {
    this.cacheTimeMs = cacheTimeMs;
  }

  @Override
  public V get(K1 key1, K2 key2) {
    cleanupCache(System.currentTimeMillis());

    return values.get(new Pair<>(key1, key2));
  }

  @Override
  public Collection<V> getAll(K1 key1) {
    cleanupCache(System.currentTimeMillis());
    Set<K2> key2s = secondLevelKeys.get(key1);
    if (key2s == null)
      return new ArrayList<>();

    List<V> res = new ArrayList<>();
    for (K2 k2 : key2s) {
      V value = values.get(new Pair<>(key1, k2));
      if (value != null)
        res.add(value);
    }

    return res;
  }

  @Override
  public int size() {
    cleanupCache(System.currentTimeMillis());
    return values.size();
  }

  @Override
  public boolean offer(K1 key1, K2 key2, V value) {
    cleanupCache(System.currentTimeMillis());

    Pair<K1, K2> keyPair = new Pair<>(key1, key2);

    if (values.putIfAbsent(keyPair, value) != null)
      // value set already (perhaps by a concurrent thread?)
      return false;

    timeouts.add(new Triple<>(System.currentTimeMillis() + cacheTimeMs, key1, key2));
    secondLevelKeys.compute(key1, (k, v) -> {
      if (v == null)
        return new ConcurrentSkipListSet<>(Arrays.asList(key2));

      Set<K2> res = new ConcurrentSkipListSet<>(v);
      res.add(key2);
      return res;
    });
    return true;
  }

  @Override
  public void delete(K1 key1, K2 key2) {
    values.remove(new Pair<>(key1, key2));
    // timeouts and secondLevelKeys will be cleaned up by cleanupCache later.
  }

  private void cleanupCache(long now) {
    PeekingIterator<Triple<Long, K1, K2>> it = Iterators.peekingIterator(timeouts.iterator());
    while (it.hasNext() && it.peek().getLeft() < now) {
      Triple<Long, K1, K2> t = it.next();
      values.remove(new Pair<>(t.getMiddle(), t.getRight()));
      secondLevelKeys.computeIfPresent(t.getMiddle(), (k, v) -> {
        Set<K2> res = new ConcurrentSkipListSet<>(v);
        res.remove(t.getRight());
        if (res.isEmpty())
          return null;
        return res;
      });
      it.remove();
    }
  }

}
