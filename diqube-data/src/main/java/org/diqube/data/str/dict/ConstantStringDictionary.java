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
package org.diqube.data.str.dict;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.diqube.data.Dictionary;

/**
 * A {@link StringDictionary} that can hold a single constant value/id pair.
 *
 * @author Bastian Gloeckle
 */
public class ConstantStringDictionary implements StringDictionary {
  private String value;
  private long id;

  public ConstantStringDictionary(String value, long id) {
    this.value = value;
    this.id = id;
  }

  @Override
  public String decompressValue(long id) throws IllegalArgumentException {
    if (id != this.id)
      throw new IllegalArgumentException("Id available: " + this.id + " but queried: " + id);
    return value;
  }

  @Override
  public String[] decompressValues(Long[] id) throws IllegalArgumentException {
    String[] res = new String[id.length];
    for (int i = 0; i < res.length; i++)
      res[i] = decompressValue(id[i]);
    return res;
  }

  @Override
  public long findIdOfValue(String value) throws IllegalArgumentException {
    if (value.equals(this.value))
      return id;
    throw new IllegalArgumentException("Value '" + value + "' not available.");
  }

  @Override
  public Long[] findIdsOfValues(String[] sortedValues) {
    Long[] res = new Long[sortedValues.length];
    for (int i = 0; i < res.length; i++) {
      if (this.value.equals(sortedValues[i]))
        res[i] = id;
      else
        res[i] = -1L;
    }
    return res;
  }

  @Override
  public Long findGtEqIdOfValue(String value) {
    int compareRes = value.compareTo(this.value);
    if (compareRes == 0)
      return id;
    if (compareRes < 0)
      return -(id + 1);
    return null;
  }

  @Override
  public Long findLtEqIdOfValue(String value) {
    int compareRes = value.compareTo(this.value);
    if (compareRes == 0)
      return id;
    if (compareRes > 0)
      return -(id + 1);
    return null;
  }

  @Override
  public boolean containsAnyValue(String[] sortedValues) {
    for (String val : sortedValues)
      if (this.value.equals(val))
        return true;
    return false;
  }

  @Override
  public boolean containsAnyValueGtEq(String value) {
    Long gtEqId = findGtEqIdOfValue(value);
    return gtEqId != null;
  }

  @Override
  public boolean containsAnyValueGt(String value) {
    Long gtEqId = findGtEqIdOfValue(value);
    return gtEqId != null && gtEqId < 0;
  }

  @Override
  public boolean containsAnyValueLtEq(String value) {
    Long ltEqId = findLtEqIdOfValue(value);
    return ltEqId != null;
  }

  @Override
  public boolean containsAnyValueLt(String value) {
    Long ltEqId = findLtEqIdOfValue(value);
    return ltEqId != null && ltEqId < 0;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(String value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueGtEq(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesGt(String value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueGt(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLt(String value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueLt(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(String value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueLtEq(value))
      res.add(id);
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<String> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    try {
      long otherId = otherDict.findIdOfValue(this.value);
      res.put(this.id, otherId);
    } catch (IllegalArgumentException e) {
      // swallow, return empty map.
    }
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<String> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long ltEqOtherId = otherDict.findLtEqIdOfValue(this.value);
    if (ltEqOtherId != null)
      res.put(this.id, ltEqOtherId);
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<String> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long gtEqOtherId = otherDict.findGtEqIdOfValue(this.value);
    if (gtEqOtherId != null)
      res.put(this.id, gtEqOtherId);
    return res;
  }

  public String getValue() {
    return value;
  }

  public long getId() {
    return id;
  }

}