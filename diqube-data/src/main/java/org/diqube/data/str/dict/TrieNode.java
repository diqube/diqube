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

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationDelegationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieNode;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieParentNode;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieTerminalNode;
import org.diqube.data.str.dict.TrieNode.TrieNodeDeserializationDelegation;
import org.diqube.util.Pair;

/**
 * A TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
 */
@DataSerializable(thriftClass = SStringDictionaryTrieNode.class,
    deserializationDelegationManager = TrieNodeDeserializationDelegation.class)
public abstract class TrieNode<T extends TBase<?, ?>> implements DataSerialization<T> {
  /**
   * @return Size in bytes of the whole sub-trie. Approx only.
   */
  public abstract long calculateApproximateSizeInBytes();

  /**
   * A {@link DataSerializationDelegationManager} for trie nodes.
   */
  public static class TrieNodeDeserializationDelegation
      implements DataSerializationDelegationManager<SStringDictionaryTrieNode> {
    @Override
    public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(
        SStringDictionaryTrieNode serialized) throws DeserializationException {
      if (serialized.isSetParentNode())
        return new Pair<>(ParentNode.class, serialized.getParentNode());
      if (serialized.isSetTerminalNode())
        return new Pair<>(TerminalNode.class, serialized.getTerminalNode());

      throw new DeserializationException("Unknown trie node type.");
    }

    @Override
    public <O extends TBase<?, ?>> SStringDictionaryTrieNode serializeWrapObject(O obj) throws SerializationException {
      SStringDictionaryTrieNode res = new SStringDictionaryTrieNode();
      if (obj instanceof SStringDictionaryTrieParentNode)
        res.setParentNode((SStringDictionaryTrieParentNode) obj);
      else if (obj instanceof SStringDictionaryTrieTerminalNode)
        res.setTerminalNode((SStringDictionaryTrieTerminalNode) obj);
      else
        throw new SerializationException("Cannot wrap " + obj);
      return res;
    }
  }
}