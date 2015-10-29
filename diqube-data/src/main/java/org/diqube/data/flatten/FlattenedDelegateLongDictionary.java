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
import java.util.NavigableMap;
import java.util.Set;

import org.apache.thrift.TBase;
import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.util.Pair;

/**
 * A {@link LongDictionary} whose delegate can be changed after it's created.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class FlattenedDelegateLongDictionary implements LongDictionary<TBase<?, ?>> {

  private LongDictionary<?> delegate;

  /* package */ FlattenedDelegateLongDictionary(LongDictionary<?> delegate) {
    this.delegate = delegate;
  }

  /**
   * Only to be called before the dictionary is used!
   */
  public void setDelegate(LongDictionary<?> delegate) {
    this.delegate = delegate;
  }

  public LongDictionary<?> getDelegate() {
    return this.delegate;
  }

  @Override
  public Long getMaxId() {
    return delegate.getMaxId();
  }

  @Override
  public Long decompressValue(long id) throws IllegalArgumentException {
    return delegate.decompressValue(id);
  }

  @Override
  public Long[] decompressValues(Long[] id) throws IllegalArgumentException {
    return delegate.decompressValues(id);
  }

  @Override
  public long findIdOfValue(Long value) throws IllegalArgumentException {
    return delegate.findIdOfValue(value);
  }

  @Override
  public Long[] findIdsOfValues(Long[] sortedValues) {
    return delegate.findIdsOfValues(sortedValues);
  }

  @Override
  public Long findGtEqIdOfValue(Long value) {
    return delegate.findGtEqIdOfValue(value);
  }

  @Override
  public Long findLtEqIdOfValue(Long value) {
    return delegate.findLtEqIdOfValue(value);
  }

  @Override
  public boolean containsAnyValue(Long[] sortedValues) {
    return delegate.containsAnyValue(sortedValues);
  }

  @Override
  public boolean containsAnyValueGtEq(Long value) {
    return delegate.containsAnyValueGtEq(value);
  }

  @Override
  public boolean containsAnyValueGt(Long value) {
    return delegate.containsAnyValueGt(value);
  }

  @Override
  public boolean containsAnyValueLtEq(Long value) {
    return delegate.containsAnyValueLtEq(value);
  }

  @Override
  public boolean containsAnyValueLt(Long value) {
    return delegate.containsAnyValueLt(value);
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Long value) {
    return delegate.findIdsOfValuesGtEq(value);
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Long value) {
    return delegate.findIdsOfValuesGt(value);
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Long value) {
    return delegate.findIdsOfValuesLt(value);
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Long value) {
    return delegate.findIdsOfValuesLtEq(value);
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Long> otherDict) {
    return delegate.findEqualIds(otherDict);
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Long> otherDict) {
    return delegate.findGtEqIds(otherDict);
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Long> otherDict) {
    return delegate.findLtEqIds(otherDict);
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + //
        delegate.calculateApproximateSizeInBytes();
  }

  @Override
  public Iterator<Pair<Long, Long>> iterator() {
    return delegate.iterator();
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, TBase<?, ?> target)
      throws SerializationException {
    throw new SerializationException("Cannot serialize flattened dict.");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      TBase<?, ?> source) throws DeserializationException {
    throw new DeserializationException("Cannot deserialize flattened dict.");
  }
}
