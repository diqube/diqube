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
package org.diqube.data.dictionary;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationDelegationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SDictionary;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionary;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionaryConstant;
import org.diqube.data.serialize.thrift.v1.SDoubleDictionaryFpc;
import org.diqube.data.serialize.thrift.v1.SLongDictionary;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryArray;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryConstant;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryEmpty;
import org.diqube.data.serialize.thrift.v1.SStringDictionary;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryConstant;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrie;
import org.diqube.data.types.dbl.dict.ConstantDoubleDictionary;
import org.diqube.data.types.dbl.dict.FpcDoubleDictionary;
import org.diqube.data.types.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.EmptyLongDictionary;
import org.diqube.data.types.str.dict.ConstantStringDictionary;
import org.diqube.data.types.str.dict.TrieStringDictionary;
import org.diqube.util.Pair;

/**
 * A {@link DataSerializationDelegationManager} for {@link SerializableDictionary}.
 *
 * @author Bastian Gloeckle
 */
public class DictionaryDeserializationDelegationManager implements DataSerializationDelegationManager<SDictionary> {

  @Override
  public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(SDictionary serialized)
      throws DeserializationException {
    if (serialized.isSetStringDict()) {
      if (serialized.getStringDict().isSetConstant())
        return new Pair<>(ConstantStringDictionary.class, serialized.getStringDict().getConstant());
      if (serialized.getStringDict().isSetTrie())
        return new Pair<>(TrieStringDictionary.class, serialized.getStringDict().getTrie());
      throw new DeserializationException("Unkown string dictionary type");
    }

    if (serialized.isSetLongDict()) {
      if (serialized.getLongDict().isSetConstant())
        return new Pair<>(ConstantLongDictionary.class, serialized.getLongDict().getConstant());
      if (serialized.getLongDict().isSetEmpty())
        return new Pair<>(EmptyLongDictionary.class, serialized.getLongDict().getEmpty());
      if (serialized.getLongDict().isSetArr())
        return new Pair<>(ArrayCompressedLongDictionary.class, serialized.getLongDict().getArr());
      throw new DeserializationException("Unkown Long dictionary type");
    }

    if (serialized.isSetDoubleDict()) {
      if (serialized.getDoubleDict().isSetConstant())
        return new Pair<>(ConstantDoubleDictionary.class, serialized.getDoubleDict().getConstant());
      if (serialized.getDoubleDict().isSetFpc())
        return new Pair<>(FpcDoubleDictionary.class, serialized.getDoubleDict().getFpc());
      throw new DeserializationException("Unkown Double dictionary type");
    }

    throw new DeserializationException("Unkown dictionary type");
  }

  @Override
  public <O extends TBase<?, ?>> SDictionary serializeWrapObject(O obj) throws SerializationException {
    SDictionary res = new SDictionary();
    if (obj instanceof SLongDictionaryConstant)
      res.setLongDict(new SLongDictionary(SLongDictionary._Fields.CONSTANT, obj));
    else if (obj instanceof SLongDictionaryEmpty)
      res.setLongDict(new SLongDictionary(SLongDictionary._Fields.EMPTY, obj));
    else if (obj instanceof SLongDictionaryArray)
      res.setLongDict(new SLongDictionary(SLongDictionary._Fields.ARR, obj));
    else if (obj instanceof SStringDictionaryConstant)
      res.setStringDict(new SStringDictionary(SStringDictionary._Fields.CONSTANT, obj));
    else if (obj instanceof SStringDictionaryTrie)
      res.setStringDict(new SStringDictionary(SStringDictionary._Fields.TRIE, obj));
    else if (obj instanceof SDoubleDictionaryConstant)
      res.setDoubleDict(new SDoubleDictionary(SDoubleDictionary._Fields.CONSTANT, obj));
    else if (obj instanceof SDoubleDictionaryFpc)
      res.setDoubleDict(new SDoubleDictionary(SDoubleDictionary._Fields.FPC, obj));
    else
      throw new SerializationException("Cannot wrap " + obj);

    return res;
  }

}
