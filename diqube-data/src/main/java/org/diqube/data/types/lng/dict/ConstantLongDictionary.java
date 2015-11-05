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
package org.diqube.data.types.lng.dict;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.flatten.AdjustableConstantLongDictionary;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryConstant;
import org.diqube.util.Pair;

import com.google.common.collect.Iterators;

/**
 * A {@link LongDictionary} which was found to contain only a single value/id combination during compression.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SLongDictionaryConstant.class)
public class ConstantLongDictionary
    implements LongDictionary<SLongDictionaryConstant>, AdjustableConstantLongDictionary<SLongDictionaryConstant> {

  private long decompressedValue;
  private long id = 0L;

  /** for deserialization */
  public ConstantLongDictionary() {

  }

  public ConstantLongDictionary(long decompressedValue) {
    this.decompressedValue = decompressedValue;
  }

  @Override
  public void setValue(long value) {
    // needed for flattenning!
    decompressedValue = value;
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

  @Override
  public void serialize(DataSerializationHelper mgr, SLongDictionaryConstant target) throws SerializationException {
    target.setId(id);
    target.setValue(decompressedValue);
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SLongDictionaryConstant source) throws DeserializationException {
    id = source.getId();
    decompressedValue = source.getValue();
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this.
        4; // 4 bytes long.
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Pair<Long, Long>> iterator() {
    return Iterators.forArray(new Pair<>(id, decompressedValue));
  }

}
