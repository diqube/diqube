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
package org.diqube.loader.compression;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.diqube.data.str.dict.StringDictionary;
import org.diqube.data.str.dict.TrieNode;
import org.diqube.data.str.dict.TrieNode.TerminalNode;
import org.diqube.data.str.dict.TrieStringDictionary;
import org.diqube.util.Pair;
import org.diqube.util.SortedSetUnionStreamSupplier;

import com.google.common.base.Strings;

/**
 * Builds a compressed string dictionary out of a map that contains values and temporary ids.
 *
 * @author Bastian Gloeckle
 */
public class CompressedStringDictionaryBuilder {
  private NavigableMap<String, Long> entityMap;

  /**
   * @param entityMap
   *          From decompressed string value to temporary Column Value IDs that have been assigned already.
   */
  public CompressedStringDictionaryBuilder fromEntityMap(NavigableMap<String, Long> entityMap) {
    this.entityMap = entityMap;
    return this;
  }

  /**
   * Build the dictionary.
   * 
   * @return {@link Pair} containing the new {@link StringDictionary} and an ID change map (maps from temporary ID that
   *         was provided in {@link #fromEntityMap(Map)} to the final ID assigned in the resulting dict).
   */
  public Pair<StringDictionary, Map<Long, Long>> build() {
    SortedSet<String> keys = (SortedSet<String>) entityMap.keySet();

    Map<Long, Long> idMap = new HashMap<>();
    long newId = 0;
    for (String key : keys) {
      long curId = newId++;
      if (entityMap.get(key) != curId)
        idMap.put(entityMap.get(key), curId);
    }

    ConstructionParentNode root = new ConstructionParentNode();
    ConstructionParentNode curNode = root;
    String curNodePrefix = "";

    newId = 0;
    // note that the keys are traversed in sorted order already!
    for (String stringValue : keys) {

      // go up the current tree until our prefix matches, this might go up as far as the root node!
      while (!stringValue.startsWith(curNodePrefix)) {
        curNodePrefix = curNodePrefix.substring(0, curNodePrefix.length() - curNode.getParentToThisStringLength());
        curNode = curNode.getParent();
      }

      String remaining = stringValue.substring(curNodePrefix.length(), stringValue.length()).intern();

      // check if there is a key that has a common prefix with our key. Note that there can be only one such key! See
      // class comment of TrieStringDictionary for why this is true.
      List<String> possiblyInterestingKeys = new LinkedList<>();
      possiblyInterestingKeys.add(curNode.getChildTerminals().floorKey(remaining));
      possiblyInterestingKeys.add(curNode.getChildTerminals().ceilingKey(remaining));
      possiblyInterestingKeys.add(curNode.getChildNodes().floorKey(remaining));
      possiblyInterestingKeys.add(curNode.getChildNodes().ceilingKey(remaining));
      String interestingKey = null;
      String interestingCommonPrefix = null;
      for (String possiblyInterestingKey : possiblyInterestingKeys) {
        if (possiblyInterestingKey == null || possiblyInterestingKey.equals(""))
          // ignore the empty-string-terminal nodes - they will not match our new string.
          continue;

        String tmp = Strings.commonPrefix(possiblyInterestingKey, remaining);
        if (!"".equals(tmp)) {
          interestingKey = possiblyInterestingKey;
          interestingCommonPrefix = tmp.intern();
          break;
        }
      }

      if (interestingKey != null) {
        // we found an entry with a common prefix - create new parent node and move the old node there and our new
        // string, too.
        ConstructionParentNode newParent = new ConstructionParentNode();
        newParent.setParent(curNode);
        newParent.setParentToThisStringLength(interestingCommonPrefix.length());
        newParent.getChildTerminals().put(removePrefix(remaining, interestingCommonPrefix), new TerminalNode(newId++));

        if (curNode.getChildNodes().containsKey(interestingKey)) {
          ConstructionParentNode nodeToMove = curNode.getChildNodes().get(interestingKey);
          nodeToMove
              .setParentToThisStringLength(nodeToMove.getParentToThisStringLength() - interestingCommonPrefix.length());
          newParent.getChildNodes().put(removePrefix(interestingKey, interestingCommonPrefix), nodeToMove);
          curNode.getChildNodes().remove(interestingKey);
        } else {
          // curNode.getChildTerminals().containsKey(interestingKey)
          newParent.getChildTerminals().put(removePrefix(interestingKey, interestingCommonPrefix),
              curNode.getChildTerminals().get(interestingKey));
          curNode.getChildTerminals().remove(interestingKey);
        }

        curNode.getChildNodes().put(interestingCommonPrefix, newParent);

        // continue working in the new parent.
        curNode = newParent;
        curNodePrefix += interestingCommonPrefix;
      } else {
        // there was no node with a common prefix. add a new terminal node!
        curNode.getChildTerminals().put(remaining, new TerminalNode(newId++));
      }
    }

    TrieStringDictionary res = new TrieStringDictionary(root.constructFinalNode(), entityMap.firstKey(),
        entityMap.lastKey(), entityMap.size() - 1);
    return new Pair<>(res, idMap);
  }

  private String removePrefix(String orig, String prefix) {
    if (prefix.length() == orig.length())
      return "".intern();
    return orig.substring(prefix.length(), orig.length()).intern();
  }

  /**
   * Just like a {@link ParentNode}, but with additional information that is required while building the trie.
   * 
   * After building the trie, for an instance of this class the real {@link ParentNode} can be created using
   * {@link #constructFinalNode()}.
   */
  private static class ConstructionParentNode extends TrieNode {
    private int parentToThisStringLength;
    private ConstructionParentNode parent;
    private NavigableMap<String, ConstructionParentNode> childNodes = new TreeMap<>();
    private NavigableMap<String, TerminalNode> childTerminals = new TreeMap<>();

    public NavigableMap<String, ConstructionParentNode> getChildNodes() {
      return childNodes;
    }

    public NavigableMap<String, TerminalNode> getChildTerminals() {
      return childTerminals;
    }

    public ConstructionParentNode getParent() {
      return parent;
    }

    public int getParentToThisStringLength() {
      return parentToThisStringLength;
    }

    public void setParentToThisStringLength(int parentToThisStringLength) {
      this.parentToThisStringLength = parentToThisStringLength;
    }

    public void setParent(ConstructionParentNode parent) {
      this.parent = parent;
    }

    /**
     * @return The actual {@link ParentNode} object for this {@link ConstructionParentNode}. This method actually
     *         returns the recursive result, where all child nodes are created and returned, too - correctly wired of
     *         course.
     */
    public ParentNode constructFinalNode() {
      Function<String, TrieNode> getFinalTrieNode = new Function<String, TrieNode>() {
        @Override
        public TrieNode apply(String key) {
          if (childTerminals.containsKey(key))
            return childTerminals.get(key);
          return childNodes.get(key).constructFinalNode();
        }
      };

      Supplier<Stream<String>> allKeyStream = new SortedSetUnionStreamSupplier<>( //
          (SortedSet<String>) this.childNodes.keySet(), (SortedSet<String>) this.childTerminals.keySet());

      TrieNode[] childNodes = allKeyStream.get().map(getFinalTrieNode).toArray(l -> new TrieNode[l]);
      char[][] childChars = allKeyStream.get().map(s -> s.toCharArray()).toArray(l -> new char[l][]);

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
  }
}
