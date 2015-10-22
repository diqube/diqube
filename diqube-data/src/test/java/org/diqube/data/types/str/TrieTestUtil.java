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
package org.diqube.data.types.str;

import java.util.stream.Stream;

import org.diqube.data.types.str.dict.ParentNode;
import org.diqube.data.types.str.dict.TerminalNode;
import org.diqube.data.types.str.dict.TrieNode;
import org.diqube.data.types.str.dict.TrieStringDictionary;
import org.diqube.util.Pair;

/**
 * Test utility for {@link TrieStringDictionary}.
 *
 * @author Bastian Gloeckle
 */
public class TrieTestUtil {

  public static ParentNode parent(Pair<String, TrieNode<?>>... children) {
    char[][] childChars =
        Stream.of(children).map(p -> p.getLeft()).map(s -> s.toCharArray()).toArray(l -> new char[l][]);
    TrieNode<?>[] childNodes = Stream.of(children).map(p -> p.getRight()).toArray(l -> new TrieNode[l]);
    long minId, maxId;
    if (childNodes[0] instanceof TerminalNode)
      minId = ((TerminalNode) childNodes[0]).getTerminalId();
    else
      minId = ((ParentNode) childNodes[0]).getMinId();

    if (childNodes[childNodes.length - 1] instanceof TerminalNode)
      maxId = ((TerminalNode) childNodes[childNodes.length - 1]).getTerminalId();
    else
      maxId = ((ParentNode) childNodes[childNodes.length - 1]).getMaxId();

    return new ParentNode(childChars, childNodes, minId, maxId);
  }

  public static TerminalNode terminal(long terminalId) {
    return new TerminalNode(terminalId);
  }
}
