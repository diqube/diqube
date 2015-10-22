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
package org.diqube.data.types.str.dict;

import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieTerminalNode;

/**
 * A terminal TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
 */
@DataSerializable(thriftClass = SStringDictionaryTrieTerminalNode.class)
public class TerminalNode extends TrieNode<SStringDictionaryTrieTerminalNode> {
  private long terminalId;

  /** for deserialization */
  public TerminalNode() {

  }

  public TerminalNode(long terminalId) {
    this.terminalId = terminalId;
  }

  public long getTerminalId() {
    return terminalId;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SStringDictionaryTrieTerminalNode target)
      throws SerializationException {
    target.setTerminalId(terminalId);
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SStringDictionaryTrieTerminalNode source)
      throws DeserializationException {
    terminalId = source.getTerminalId();
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this
        8;
  }
}