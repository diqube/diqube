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
package org.diqube.data.lng.dict;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.diqube.data.Dictionary;

/**
 * An empty {@link LongDictionary}.
 *
 * @author Bastian Gloeckle
 */
public class EmptyLongDictionary implements LongDictionary {

  @Override
  public Long decompressValue(long id) throws IllegalArgumentException {
    throw new IllegalArgumentException("Directory is emtpy");
  }

  @Override
  public Long[] decompressValues(Long[] id) throws IllegalArgumentException {
    throw new IllegalArgumentException("Directory is emtpy");
  }

  @Override
  public long findIdOfValue(Long value) throws IllegalArgumentException {
    throw new IllegalArgumentException("Directory is emtpy");
  }

  @Override
  public Long findGtEqIdOfValue(Long value) {
    return null;
  }

  @Override
  public Long findLtEqIdOfValue(Long value) {
    return null;
  }

  @Override
  public Long[] findIdsOfValues(Long[] sortedValues) {
    Long[] res = new Long[sortedValues.length];
    Arrays.fill(res, -1L);
    return res;
  }

  @Override
  public boolean containsAnyValue(Long[] sortedValues) {
    return false;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Long> otherDict) {
    return new TreeMap<>();
  }

  @Override
  public boolean containsAnyValueGtEq(Long value) {
    return false;
  }

  @Override
  public boolean containsAnyValueGt(Long value) {
    return false;
  }

  @Override
  public boolean containsAnyValueLtEq(Long value) {
    return false;
  }

  @Override
  public boolean containsAnyValueLt(Long value) {
    return false;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Long value) {
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Long value) {
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Long value) {
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Long value) {
    return new HashSet<>();
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Long> otherDict) {
    return new TreeMap<>();
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Long> otherDict) {
    return new TreeMap<>();
  }

  @Override
  public Long getMaxId() {
    return null;
  }

}
