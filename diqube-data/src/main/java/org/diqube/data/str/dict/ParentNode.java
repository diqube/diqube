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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.DataSerialization.DataSerializationHelper;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieNode;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieParentNode;

/**
 * A parent TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
 */
@DataSerializable(thriftClass = SStringDictionaryTrieParentNode.class)
public class ParentNode extends TrieNode<SStringDictionaryTrieParentNode> {
  private TrieNode<?>[] childNodes;
  // sorted!
  private char[][] childChars;
  private long minId;
  private long maxId;

  /** for deserialization */
  public ParentNode() {

  }

  public ParentNode(char[][] childChars, TrieNode<?>[] childNodes, long minId, long maxId) {
    this.childChars = childChars;
    this.childNodes = childNodes;
    this.minId = minId;
    this.maxId = maxId;
  }

  public TrieNode<?>[] getChildNodes() {
    return childNodes;
  }

  public char[][] getChildChars() {
    return childChars;
  }

  public long getMinId() {
    return minId;
  }

  public long getMaxId() {
    return maxId;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SStringDictionaryTrieParentNode target)
      throws SerializationException {
    target.setMaxId(maxId);
    target.setMinId(minId);
    Map<String, SStringDictionaryTrieNode> serializedChildNodes = new HashMap<>();
    for (int i = 0; i < childNodes.length; i++)
      serializedChildNodes.put(new String(childChars[i]),
          mgr.serializeChild(SStringDictionaryTrieNode.class, childNodes[i]));
    target.setChildNodes(serializedChildNodes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SStringDictionaryTrieParentNode source)
      throws DeserializationException {
    minId = source.getMinId();
    maxId = source.getMaxId();
    childNodes = new TrieNode[source.getChildNodes().size()];
    childChars = new char[source.getChildNodes().size()][];
    int i = 0;
    for (String serializedChildChars : source.getChildNodes().keySet().stream().collect(Collectors.toList())) {
      childChars[i] = serializedChildChars.toCharArray();
      childNodes[i] = mgr.deserializeChild(TrieNode.class, source.getChildNodes().get(serializedChildChars));
      i++;
    }
  }
}