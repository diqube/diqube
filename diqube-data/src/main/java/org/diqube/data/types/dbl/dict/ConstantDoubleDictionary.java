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
package org.diqube.data.types.dbl.dict;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionaryConstant;
import org.diqube.util.Pair;

import com.google.common.collect.Iterators;

/**
 * A {@link DoubleDictionary} that can hold a single constant value/id pair.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SDoubleDictionaryConstant.class)
public class ConstantDoubleDictionary implements DoubleDictionary<SDoubleDictionaryConstant> {
  private Double value;
  private long id = 0L;

  /** for deserialization */
  public ConstantDoubleDictionary() {

  }

  public ConstantDoubleDictionary(Double value) {
    this.value = value;
  }

  @Override
  public Double decompressValue(long id) throws IllegalArgumentException {
    if (id != this.id)
      throw new IllegalArgumentException("Id available: " + this.id + " but queried: " + id);
    return value;
  }

  @Override
  public Double[] decompressValues(Long[] id) throws IllegalArgumentException {
    Double[] res = new Double[id.length];
    for (int i = 0; i < res.length; i++)
      res[i] = decompressValue(id[i]);
    return res;
  }

  @Override
  public long findIdOfValue(Double value) throws IllegalArgumentException {
    if (value.equals(this.value))
      return id;
    throw new IllegalArgumentException("Value '" + value + "' not available.");
  }

  @Override
  public Long[] findIdsOfValues(Double[] sortedValues) {
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
  public Long findGtEqIdOfValue(Double value) {
    int compareRes = value.compareTo(this.value);
    if (compareRes == 0)
      return id;
    if (compareRes < 0)
      return -(id + 1);
    return null;
  }

  @Override
  public Long findLtEqIdOfValue(Double value) {
    int compareRes = value.compareTo(this.value);
    if (compareRes == 0)
      return id;
    if (compareRes > 0)
      return -(id + 1);
    return null;
  }

  @Override
  public boolean containsAnyValue(Double[] sortedValues) {
    for (Double val : sortedValues)
      if (this.value.equals(val))
        return true;
    return false;
  }

  @Override
  public boolean containsAnyValueGtEq(Double value) {
    Long gtEqId = findGtEqIdOfValue(value);
    return gtEqId != null;
  }

  @Override
  public boolean containsAnyValueGt(Double value) {
    Long gtEqId = findGtEqIdOfValue(value);
    return gtEqId != null && gtEqId < 0;
  }

  @Override
  public boolean containsAnyValueLtEq(Double value) {
    Long ltEqId = findLtEqIdOfValue(value);
    return ltEqId != null;
  }

  @Override
  public boolean containsAnyValueLt(Double value) {
    Long ltEqId = findLtEqIdOfValue(value);
    return ltEqId != null && ltEqId < 0;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Double value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueGtEq(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Double value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueGt(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Double value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueLt(value))
      res.add(id);
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Double value) {
    Set<Long> res = new HashSet<>();
    if (containsAnyValueLtEq(value))
      res.add(id);
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Double> otherDict) {
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
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Double> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long ltEqOtherId = otherDict.findLtEqIdOfValue(this.value);
    if (ltEqOtherId != null)
      res.put(this.id, ltEqOtherId);
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Double> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    Long gtEqOtherId = otherDict.findGtEqIdOfValue(this.value);
    if (gtEqOtherId != null)
      res.put(this.id, gtEqOtherId);
    return res;
  }

  public Double getValue() {
    return value;
  }

  public long getId() {
    return id;
  }

  @Override
  public Long getMaxId() {
    return id;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SDoubleDictionaryConstant target) throws SerializationException {
    target.setId(id);
    target.setValue(value);
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SDoubleDictionaryConstant source)
      throws DeserializationException {
    id = source.getId();
    value = source.getValue();
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this.
        8 + 16; // 8 bytes double + 16 byte object header
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Pair<Long, Double>> iterator() {
    return Iterators.forArray(new Pair<>(id, value));
  }

}