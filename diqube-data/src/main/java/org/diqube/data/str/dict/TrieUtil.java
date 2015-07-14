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

import org.diqube.data.str.dict.TrieNode.ParentNode;
import org.diqube.data.str.dict.TrieNode.TerminalNode;

/**
 * Utility class for handling tries (see {@link TrieNode}).
 *
 * @author Bastian Gloeckle
 */
public class TrieUtil {

  /**
   * Inspects a trie that is built by the given node and finds the {@link TerminalNode#getTerminalId()} of a specific
   * value.
   * 
   * @param str
   *          The value to be searched.
   * @param fromIndex
   *          The number of leading characters to ignore in str. If this is > 0 then the corresponding characters are
   *          not inspected in this method, but it pretends that the str array starts at the given index.
   * @param curNode
   *          The node that starts the trie.
   * @return If >= 0, then it is the ID of {@link TerminalNode} that corresponds to the searched value. If < 0, then the
   *         searched value is not contained in the trie. The result is then -(ip +1), where <code>ip</code> is the
   *         insertion point at which the searched value should be inserted (-> ip = ID of next greater terminal id).
   */
  public static long findIdOfValue(char[] str, int fromIndex, TrieNode curNode) {
    if (curNode instanceof TerminalNode) {
      if (fromIndex == str.length)
        return ((TerminalNode) curNode).getTerminalId();
      // we are at a terminal node but not all of our input string was matched -> string is not contained.
      // Insert Point would be the "ID of the terminal node we found +1" (as the terminal node is < than our string).
      return -(((TerminalNode) curNode).getTerminalId() + 2);
    }
    ParentNode parent = (ParentNode) curNode;

    // TODO #6 do not compare to all children, but do some sort of binary search.
    for (int i = 0; i < parent.getChildNodes().length; i++) {
      char[] subSeq = parent.getChildChars()[i];
      int compareRes = compareChars(subSeq, str, fromIndex);
      if (compareRes == 0) {
        // matched whole subSeq, continue recursively.
        return findIdOfValue(str, fromIndex + subSeq.length, parent.getChildNodes()[i]);
      }
      if (compareRes > 0) {
        // as soon as the subSeq gets greater than the searched string, we can stop, as subSeq are sorted!

        // find insertion point of the searched string, as it is not contained in the dict.
        long insertionPoint;
        if (parent.getChildNodes()[i] instanceof TerminalNode)
          insertionPoint = ((TerminalNode) parent.getChildNodes()[i]).getTerminalId();
        else
          insertionPoint = ((ParentNode) parent.getChildNodes()[i]).getMinId();

        return -(insertionPoint + 1);
      }
    }

    // we get here only, if all child nodes chars were < than the searched string.
    // -> InsertionPoint would be "lastChild.getMaxId() + 1".
    long insertionPoint;
    int lastChild = parent.getChildNodes().length - 1;
    if (parent.getChildNodes()[lastChild] instanceof TerminalNode)
      insertionPoint = ((TerminalNode) parent.getChildNodes()[lastChild]).getTerminalId() + 1;
    else
      insertionPoint = ((ParentNode) parent.getChildNodes()[lastChild]).getMaxId() + 1;

    return -(insertionPoint + 1);
  }

  /**
   * Compare two character arrays with at most comparing array1.length number of characters.
   * 
   * @param array1
   *          The first character array that will be inspected
   * @param array2
   *          The second character array that will be inspected
   * @param array2StartIdx
   *          If the second array should not be inspected from the beginning, this denotes the offset to start at.
   * @return <code>0</code> if all values of array1 are at the beginning of array2 (= they are equal up to the length of
   *         array1). Return value is positive if array1 is bigger than array2, the result value is then (eq+1) where
   *         <code>eq</code> is the number of equal characters found before identifying a character that was greater in
   *         array1. Return value is negative, if array1 is smaller than array2, the result value is then -(eq+1) where
   *         <code>eq</code> is the number of equal characters found before identifying a character that was smaller in
   *         array1.
   */
  public static int compareChars(char[] array1, char[] array2, int array2StartIdx) {
    if (array1.length == 0 && array2.length - array2StartIdx > 0)
      return -1;

    for (int i = 0; i < array1.length; i++) {
      if (array2StartIdx + i >= array2.length)
        return i + 1;

      if (array1[i] != array2[array2StartIdx + i]) {
        if (array1[i] < array2[array2StartIdx + i])
          return -(i + 1);
        return i + 1;
      }
    }
    return 0;
  }
}