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
package org.diqube.data.flatten;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.util.Pair;

/**
 *
 * @author Bastian Gloeckle
 */
public class AbstractFlattenedMergedDictionary<T> implements Dictionary<T> {

  protected AbstractFlattenedMergedDictionary(Map<Long, Dictionary<?>> delegates,
      CompressedLongArray<?> sortedDelegateIds) {

  }

  @Override
  public Iterator<Pair<Long, T>> iterator() {
    return null;
  }

  @Override
  public Long getMaxId() {
    return null;
  }

  @Override
  public T decompressValue(long id) throws IllegalArgumentException {
    return null;
  }

  @Override
  public T[] decompressValues(Long[] id) throws IllegalArgumentException {
    return null;
  }

  @Override
  public long findIdOfValue(T value) throws IllegalArgumentException {
    return 0;
  }

  @Override
  public Long[] findIdsOfValues(T[] sortedValues) {
    return null;
  }

  @Override
  public Long findGtEqIdOfValue(T value) {
    return null;
  }

  @Override
  public Long findLtEqIdOfValue(T value) {
    return null;
  }

  @Override
  public boolean containsAnyValue(T[] sortedValues) {
    return false;
  }

  @Override
  public boolean containsAnyValueGtEq(T value) {
    return false;
  }

  @Override
  public boolean containsAnyValueGt(T value) {
    return false;
  }

  @Override
  public boolean containsAnyValueLtEq(T value) {
    return false;
  }

  @Override
  public boolean containsAnyValueLt(T value) {
    return false;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(T value) {
    return null;
  }

  @Override
  public Set<Long> findIdsOfValuesGt(T value) {
    return null;
  }

  @Override
  public Set<Long> findIdsOfValuesLt(T value) {
    return null;
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(T value) {
    return null;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<T> otherDict) {
    return null;
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<T> otherDict) {
    return null;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<T> otherDict) {
    return null;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 0;
  }

}
