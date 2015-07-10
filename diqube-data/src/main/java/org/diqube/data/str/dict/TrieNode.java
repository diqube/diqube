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

/**
 * A TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
 */
public class TrieNode {
  /**
   * A parent TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
   */
  public static class ParentNode extends TrieNode {
    private TrieNode[] childNodes;
    // sorted!
    private char[][] childChars;
    private long minId;
    private long maxId;

    public ParentNode(char[][] childChars, TrieNode[] childNodes, long minId, long maxId) {
      this.childChars = childChars;
      this.childNodes = childNodes;
      this.minId = minId;
      this.maxId = maxId;
    }

    public TrieNode[] getChildNodes() {
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
  }

  /**
   * A terminal TrieNode in the trie. See class comment of {@link TrieStringDictionary}.
   */
  public static class TerminalNode extends TrieNode {
    private long terminalId;

    public TerminalNode(long terminalId) {
      this.terminalId = terminalId;
    }

    public long getTerminalId() {
      return terminalId;
    }
  }
}