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

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationDelegationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongDictionary;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryArray;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryConstant;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryEmpty;
import org.diqube.util.Pair;

/**
 * {@link DataSerializationDelegationManager} for {@link LongDictionary}.
 *
 * @author Bastian Gloeckle
 */
public class LongDictionarySerializationDelegationManager
    implements DataSerializationDelegationManager<SLongDictionary> {

  @Override
  public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(SLongDictionary serialized)
      throws DeserializationException {
    if (serialized.isSetConstant())
      return new Pair<>(ConstantLongDictionary.class, serialized.getConstant());
    if (serialized.isSetEmpty())
      return new Pair<>(EmptyLongDictionary.class, serialized.getEmpty());
    if (serialized.isSetArr())
      return new Pair<>(ArrayCompressedLongDictionary.class, serialized.getArr());
    throw new DeserializationException("Unkown Long dictionary type");
  }

  @Override
  public <O extends TBase<?, ?>> SLongDictionary serializeWrapObject(O obj) throws SerializationException {
    if (obj instanceof SLongDictionaryConstant)
      return new SLongDictionary(SLongDictionary._Fields.CONSTANT, obj);
    else if (obj instanceof SLongDictionaryEmpty)
      return new SLongDictionary(SLongDictionary._Fields.EMPTY, obj);
    else if (obj instanceof SLongDictionaryArray)
      return new SLongDictionary(SLongDictionary._Fields.ARR, obj);

    throw new SerializationException("Cannot wrap " + obj);
  }

}
