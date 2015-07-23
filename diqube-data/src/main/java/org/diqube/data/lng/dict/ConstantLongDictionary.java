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
 * A {@link LongDictionary} which was found to contain only a single value/id combination during compression.
 *
 * @author Bastian Gloeckle
 */
public class ConstantLongDictionary implements LongDictionary {

  private long decompressedValue;
  private long id;

  public ConstantLongDictionary(long decompressedValue, long id) {
    this.decompressedValue = decompressedValue;
    this.id = id;
  }

  @Override
  public Long decompressValue(long id) throws IllegalArgumentException {
    if (id != this.id)
      throw new IllegalArgumentException("Invalid ID");
    return decompressedValue;
  }

  @Override
  public Long[] decompressValues(Long[] id) throws IllegalArgumentException {
    Long[] res = new Long[id.length];
    for (int i = 0; i < res.length; i++)
      res[i] = decompressValue(id[i]);
    return res;
  }

  @Override
  public long findIdOfValue(Long value) throws IllegalArgumentException {
    if (value != this.decompressedValue)
      throw new IllegalArgumentException("Invalid value");
    return id;
  }

  @Override
  public Long findGtEqIdOfValue(Long value) {
    if (decompressedValue < value)
      return null;
    if (decompressedValue == value)
      return id;
    return -(id + 1);
  }

  @Override
  public Long findLtEqIdOfValue(Long value) {
    if (decompressedValue > value)
      return null;
    if (decompressedValue == value)
      return id;
    return -(id + 1);
  }

  @Override
  public Long[] findIdsOfValues(Long[] sortedValues) {
    Long[] res = new Long[sortedValues.length];
    for (int i = 0; i < res.length; i++)
      res[i] = (sortedValues[i] == decompressedValue) ? id : -1L;
    return res;
  }

  @Override
  public boolean containsAnyValue(Long[] sortedValues) {
    int binarySearchRes = Arrays.binarySearch(sortedValues, decompressedValue);
    return binarySearchRes >= 0;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    long otherId = otherDict.findIdOfValue(decompressedValue);
    if (otherId != -1L)
      res.put(id, otherId);

    return res;
  }

  @Override
  public boolean containsAnyValueGtEq(Long value) {
    return decompressedValue >= value;
  }

  @Override
  public boolean containsAnyValueGt(Long value) {
    return decompressedValue > value;
  }

  @Override
  public boolean containsAnyValueLtEq(Long value) {
    return decompressedValue <= value;
  }

  @Override
  public boolean containsAnyValueLt(Long value) {
    return decompressedValue < value;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Long value) {
    if (decompressedValue >= value)
      return new HashSet<>(Arrays.asList(new Long[] { id }));
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Long value) {
    if (decompressedValue > value)
      return new HashSet<>(Arrays.asList(new Long[] { id }));
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Long value) {
    if (decompressedValue < value)
      return new HashSet<>(Arrays.asList(new Long[] { id }));
    return new HashSet<>();
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Long value) {
    if (decompressedValue <= value)
      return new HashSet<>(Arrays.asList(new Long[] { id }));
    return new HashSet<>();
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long otherId = otherDict.findLtEqIdOfValue(decompressedValue);
    if (otherId == null)
      return res;

    res.put(id, otherId);

    return res;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long otherId = otherDict.findGtEqIdOfValue(decompressedValue);
    if (otherId == null)
      return res;

    res.put(id, otherId);

    return res;
  }

  public long getDecompressedValue() {
    return decompressedValue;
  }

  public long getId() {
    return id;
  }

  @Override
  public Long getMaxId() {
    return id;
  }

}
