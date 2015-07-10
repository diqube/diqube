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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;

import org.diqube.data.str.dict.TrieNode.ParentNode;
import org.diqube.data.str.dict.TrieNode.TerminalNode;

/**
 * Helper class to analyze the values of two tries and compare them to each other.
 *
 * @author Bastian Gloeckle
 */
public class TrieValueAnalyzer {

  /**
   * Traverses two Tries and identifies the terminal IDs of the tries where specific invariants hold (see
   * {@link TrieValueAnalyzerCallback}).
   * 
   * @param ourNode
   *          The root node of the first trie.
   * @param otherNode
   *          The root node of the second trie.
   * @param callback
   *          The callback that will be called when specific situations are encountered. The methods of the callback
   *          will be called for all terminal IDs of ourNode at least once. Please note that the equalIds method is the
   *          "strongest" guarantee this class can make. Both other methods "greater" and "smaller" are equally "strong"
   *          - meaning if this analyzer identifies a terminal string being greater than another, it might only call the
   *          "greater" method and not the "smaller" method for that terminal. As it is guaranteed (see JavaDoc of e.g.
   *          {@link TrieValueAnalyzerCallback#foundGreaterId(long, long)}) that the nearest ID to the terminal is
   *          returned on "greater" or "smaller" calls and assuming that the "equalId" method was not called on a given
   *          terminal, the callback can safely add/subtract 1 from the otherId to deduce the opposite inequality.
   */
  public void analyzeTries(ParentNode ourNode, ParentNode otherNode, TrieValueAnalyzerCallback callback) {
    analyzeTriesInternal(ourNode, null, otherNode, null, callback);
  }

  /**
   * Traverses two Tries and identifies the terminal IDs.
   * 
   * <p>
   * The general goal of one execution of this method is to identify any directly matching strings (which are identified
   * by early {@link TerminalNode}s in the tries), or if two {@link ParentNode}s are identified as children, this method
   * calls itself recursively.
   * 
   * @param ourNode
   *          The root node of the first trie.
   * @param ourRequiredPrefix
   *          If unset, this is <code>null</code>. Otherwise this contains a character array which all children of
   *          ourNode need to adhere to - those that don't won't be inspected.
   * @param otherNode
   *          The root node of the second trie.
   * @param otherRequiredPrefix
   *          If unset, this is <code>null</code>. Otherwise this contains a character array which all children of
   *          otherNode need to adhere to - those that don't won't be inspected.
   * @param callback
   *          The callback that will be called when specific situations are encountered. The methods of the callback
   *          will be called for all terminal IDs of ourNode at least once. Please note that the equalIds method is the
   *          "strongest" guarantee this class can make. Both other methods "greater" and "smaller" are equally "strong"
   *          - meaning if this analyzer identifies a terminal string being greater than another, it might only call the
   *          "greater" method and not the "smaller" method for that terminal. As it is guaranteed (see JavaDoc of e.g.
   *          {@link TrieValueAnalyzerCallback#foundGreaterId(long, long)}) that the nearest ID to the terminal is
   *          returned on "greater" or "smaller" calls and assuming that the "equalId" method was not called on a given
   *          terminal, the callback can safely add/subtract 1 from the otherId to deduce the opposite inequality.
   */
  private void analyzeTriesInternal(ParentNode ourNode, char[] ourRequiredPrefix, ParentNode otherNode,
      char[] otherRequiredPrefix, TrieValueAnalyzerCallback callback) {
    int ourIdx = 0;
    int otherIdx = 0;
    boolean doneOnWholeSubTrieOur = false;
    boolean doneOnWholeSubTrieOther = false;

    while (ourIdx < ourNode.getChildNodes().length && otherIdx < otherNode.getChildNodes().length) {
      // match the children of ourNode and otherNode - try to find matching pairs at ourIdx/otherIdx.

      TrieNode ourChild = ourNode.getChildNodes()[ourIdx];
      // the other side needs to have a specific prefix. So lets prefix ourChildChars accordingly.
      char[] ourChildChars = prefixIfNonNull(otherRequiredPrefix, ourNode.getChildChars()[ourIdx]);
      TrieNode otherChild = otherNode.getChildNodes()[otherIdx];
      // our side needs to have a specific prefix. So lets prefix otherChildChars accordingly.
      char[] otherChildChars = prefixIfNonNull(ourRequiredPrefix, otherNode.getChildChars()[otherIdx]);

      doneOnWholeSubTrieOur = false;
      doneOnWholeSubTrieOther = false;

      int compareRes = TrieUtil.compareChars(ourChildChars, otherChildChars, 0);
      if (compareRes == 0) {
        // whole string in ourChild was matched.
        if (ourChild instanceof TerminalNode) {
          // Our child is a Terminal node, means our tree ends here with a final string that is contained in our dict.
          if (ourChildChars.length == otherChildChars.length) {
            // we did not only match the whole string of our node, but also of the other node. If the otherNode string
            // is longer, there is no equal string, as the string in the other dict is longer than the one in our dict.
            if (otherChild instanceof TerminalNode) {
              // The otherDict also contains a TerminalNode -> we have a match!
              callback.foundEqualIds(((TerminalNode) ourChild).getTerminalId(),
                  ((TerminalNode) otherChild).getTerminalId());
              doneOnWholeSubTrieOther = true; // done, because we worked on a TerminalNode that matched perfectly.
            } else {
              // The otherDict contains the same string, but does not have a TerminalNode, but a ParentNode. It could be
              // that that ParentNode in turn contains a TerminalNode for an empty string as sub-node, which would lead
              // to a match. Check that.
              // As the full string of ourNode matched, it cannot be a match to a string in otherDict if we'd have to
              // go deeper in otherDict -> there is at max one level!
              ParentNode otherChildParent = (ParentNode) otherChild;

              if (otherChildParent.getChildChars().length > 0 && otherChildParent.getChildChars()[0].length == 0
                  && otherChildParent.getChildNodes()[0] instanceof TerminalNode) {
                TerminalNode otherChildChildTerm = (TerminalNode) otherChildParent.getChildNodes()[0];
                callback.foundEqualIds(((TerminalNode) ourChild).getTerminalId(), otherChildChildTerm.getTerminalId());
              } else
                // we fully matched ourChildChars, but otherNode contains a ParentNode that does not have a direct
                // terminalNode -> all terminals referred to by otherNode are bigger than our node.
                callback.foundGreaterId(((TerminalNode) ourChild).getTerminalId(), otherChildParent.getMinId());
            }
          } else {

            // ourChild is a terminal node whose value is a pure prefix to otherChild -> otherChild is larger.
            callback.foundGreaterId(((TerminalNode) ourChild).getTerminalId(), getMinId(otherChild));
          }

          doneOnWholeSubTrieOur = true; // done, because our trie was a terminalNode.
        } else {
          // matched whole string in ourChild, but we do not have a terminalNode as ourChild.

          // check if otherChild is a terminalNode and see if we can match that terminal string to one in ourNode.
          if (otherChild instanceof TerminalNode) {
            // find Characters in otherDict that still need to be matched.
            long ourId = TrieUtil.findIdOfValue(otherChildChars, ourChildChars.length, ourChild);
            long turnaroundPoint = (ourId < 0) ? -(ourId + 1) : ourId;
            findAllTerminalNodes(ourChild).forEach(term -> {
              if (term.getTerminalId() == ourId)
                callback.foundEqualIds(ourId, ((TerminalNode) otherChild).getTerminalId());
              else if (term.getTerminalId() < turnaroundPoint)
                callback.foundGreaterId(term.getTerminalId(), ((TerminalNode) otherChild).getTerminalId());
              else
                callback.foundSmallerId(term.getTerminalId(), ((TerminalNode) otherChild).getTerminalId());
            });
            doneOnWholeSubTrieOther = true; // matched TerminalNode otherNode fully.
          } else {
            // both, ourChild and otherChild are a ParentNode with the same prefix -> go recursive.

            if (ourChildChars.length == otherChildChars.length) {
              // string fully matched
              analyzeTriesInternal((ParentNode) ourChild, null, (ParentNode) otherChild, null, callback);

              // we worked on both sub-tries and they cannot match any other in the tries -> mark both as done.
              doneOnWholeSubTrieOther = true;
              doneOnWholeSubTrieOur = true;
            } else {
              // we matched all of ourChildChars, but otherChildChars contains more elements -> we need to set a
              // ourPrefix for the following recursive call.
              char[] newOurPrefix = new char[otherChildChars.length - ourChildChars.length];
              // TODO remove double-arraycopy, as the recursive call will copy right away again.
              System.arraycopy(otherChildChars, ourChildChars.length, newOurPrefix, 0, newOurPrefix.length);
              analyzeTriesInternal((ParentNode) ourChild, newOurPrefix, (ParentNode) otherChild, null, callback);

              // mark the trie with the more specific string as done (in this case otherChild), as the more general one
              // might actually match more sub-tries of the other node.
              doneOnWholeSubTrieOther = true;
            }
          }
        }
      } else if (compareRes < 0) {
        // not all of ourChildChars were matched, our value is smaller than the other -> there is a differing character
        // at a specific index.
        // Mark all terminalNodes as being smaller than otherChild.
        long otherMinId = getMinId(otherChild);
        findAllTerminalNodes(ourChild).forEach(term -> callback.foundGreaterId(term.getTerminalId(), otherMinId));

        // we worked on all strings contained in the trie of ourChild.
        doneOnWholeSubTrieOur = true;
      } else {
        // compareRes > 0 -> ourChildChars is greater than otherChildChars. We though might have matched all of
        // otherChildChars, in which case we'd need to recurse deeper in otherChild.

        if (otherChildChars.length == 0) {
          // special case, where ourChild has characters, but others does not - we cannot match that node! so lets skip
          // it as soon as possible.
          long otherMaxId = getMaxId(otherChild);
          findAllTerminalNodes(ourChild).forEach(term -> callback.foundSmallerId(term.getTerminalId(), otherMaxId));
          doneOnWholeSubTrieOther = true;
        } else if (compareRes > otherChildChars.length) {
          // we matched all of otherChildChars.

          if (ourChild instanceof TerminalNode) {
            // our node is a terminalNode, lets search for the remaining string in otherChild.

            long otherId = TrieUtil.findIdOfValue(ourChildChars, otherChildChars.length, otherChild);
            if (otherId >= 0)
              callback.foundEqualIds(((TerminalNode) ourChild).getTerminalId(), otherId);
            else {
              long insertionPoint = -(otherId + 1);
              if (insertionPoint > 0)
                callback.foundSmallerId(((TerminalNode) ourChild).getTerminalId(), insertionPoint - 1);
              if (insertionPoint <= getMaxId(otherChild))
                callback.foundGreaterId(((TerminalNode) ourChild).getTerminalId(), insertionPoint);
            }

            doneOnWholeSubTrieOur = true; // our trie was a TerminalNode only, so we worked on the whole trie.
          } else if (otherChild instanceof TerminalNode) {
            // other string is shorter than ours, but other node is terminal -> there are no more nodes in
            // otherChild that we could use to match our string.
            findAllTerminalNodes(ourChild).forEach(
                term -> callback.foundSmallerId(term.getTerminalId(), ((TerminalNode) otherChild).getTerminalId()));
            // go forward on that node, that was more specific - the less specific one could match additional nodes!
            if (ourChildChars.length > otherChildChars.length)
              doneOnWholeSubTrieOur = true;
            else
              doneOnWholeSubTrieOther = true;
          } else {
            // both child nodes are ParentNodes, where our ParentNode has a string that starts with the whole string of
            // otherNode.
            char[] newOtherPrefix = new char[ourChildChars.length - otherChildChars.length];
            System.arraycopy(ourChildChars, otherChildChars.length, newOtherPrefix, 0, newOtherPrefix.length);
            analyzeTriesInternal((ParentNode) ourChild, null, (ParentNode) otherChild, newOtherPrefix, callback);

            // mark the trie with the more specific string as done (in this case ourChild), as the more general one
            // might actually match more sub-tries of the other node.
            doneOnWholeSubTrieOur = true;
          }
        } else {
          // ourChildChars is greater than otherChildChars and we did not match all of the otherChildChars. This means
          // there is a conflicting character somewhere, where ourChild is > otherChild.
          long otherMaxId = getMaxId(otherChild);
          findAllTerminalNodes(ourChild).forEach(term -> callback.foundSmallerId(term.getTerminalId(), otherMaxId));

          // we worked on the full other trie, ourChild though might still match a following sub-trie of otherNode.
          doneOnWholeSubTrieOther = true;
        }
      }

      if (doneOnWholeSubTrieOther)
        otherIdx++;
      if (doneOnWholeSubTrieOur)
        ourIdx++;
    }

    // If we stopped iterating the while loop because otherNode did not have any more children, we know that all
    // children of ourNode that we did not look at are bigger than the biggest sub-node of otherNode. As we guarantee
    // that a callback method will be called for each terminalId in ourNode, we need to call the corresponding method.
    long maxOtherId = getMaxId(otherNode);
    if (!doneOnWholeSubTrieOur)
      // we did not proceed ourIdx in the last execution of the while loop - that means we worked on the corresponding
      // child already and do not need to inspect it further.
      ourIdx++;
    while (ourIdx < ourNode.getChildNodes().length) {
      findAllTerminalNodes(ourNode.getChildNodes()[ourIdx]).forEach(
          term -> callback.foundSmallerId(term.getTerminalId(), maxOtherId));
      ourIdx++;
    }
  }

  /**
   * Returns a prefixed char array, if there is a prefix specified.
   * 
   * @param prefixOrNull
   *          If not <code>null</code> this char[] will be prefixed to actualValue.
   * @param actualValue
   *          The value that should be returned with a possible prefix.
   */
  private char[] prefixIfNonNull(char[] prefixOrNull, char[] actualValue) {
    if (prefixOrNull == null)
      return actualValue;

    char[] res = new char[prefixOrNull.length + actualValue.length];
    System.arraycopy(prefixOrNull, 0, res, 0, prefixOrNull.length);
    System.arraycopy(actualValue, 0, res, prefixOrNull.length, actualValue.length);
    return res;
  }

  private Collection<TerminalNode> findAllTerminalNodes(TrieNode node) {
    Collection<TerminalNode> res = new ArrayList<>();
    Deque<TrieNode> queue = new LinkedList<>();
    queue.add(node);
    while (!queue.isEmpty()) {
      TrieNode cur = queue.poll();
      if (cur instanceof TerminalNode)
        res.add((TerminalNode) cur);
      else
        queue.addAll(Arrays.asList(((ParentNode) cur).getChildNodes()));
    }
    return res;
  }

  private long getMinId(TrieNode node) {
    if (node instanceof TerminalNode)
      return ((TerminalNode) node).getTerminalId();
    return ((ParentNode) node).getMinId();
  }

  private long getMaxId(TrieNode node) {
    if (node instanceof TerminalNode)
      return ((TerminalNode) node).getTerminalId();
    return ((ParentNode) node).getMaxId();
  }

  /**
   * Callback that is called when specific situations are encountered while executing
   * {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
   */
  public static interface TrieValueAnalyzerCallback {

    /**
     * An equal value in the tries was found.
     * 
     * @param ourId
     *          ID of the value that was reached from ourNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     * @param otherId
     *          ID of the value that was reached from otherNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     */
    public void foundEqualIds(long ourId, long otherId);

    /**
     * It was found that the value of otherId is greater than the value of ourId.
     * 
     * <p>
     * Please be aware that this method might be called multiple times with the same ourId but different otherIds.
     * {@link TrieValueAnalyzer} though guarantees for "ourIds" that are not used in a call to
     * {@link #foundEqualIds(long, long)} that either (1) this method will be called at least once for the smallest
     * otherId of the set of otherIds that are greater than a specific ourId or (2) {@link #foundSmallerId(long, long)}
     * will be called at least once with the greatest otherIds of the set of otherIds that are smaller than the value of
     * ourId.
     * 
     * @param ourId
     *          ID of the value that was reached from ourNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     * @param otherId
     *          ID of the value that was reached from otherNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     */
    public void foundGreaterId(long ourId, long otherId);

    /**
     * It was found that the value of otherId is smaller than the value of ourId.
     * 
     * <p>
     * Please be aware that this method might be called multiple times with the same ourId but different otherIds.
     * {@link TrieValueAnalyzer} though guarantees for "ourIds" that are not used in a call to
     * {@link #foundEqualIds(long, long)} that either (1) this method will be called at least once for the greatest
     * otherId of the set of otherIds that are smaller than a specific ourId or (2) {@link #foundGreaterId(long, long)}
     * will be called at least once with the smallest otherId of the set of otherIds that are greater than the value of
     * ourId.
     * 
     * @param ourId
     *          ID of the value that was reached from ourNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     * @param otherId
     *          ID of the value that was reached from otherNode parameter to
     *          {@link TrieValueAnalyzer#analyzeTries(ParentNode, ParentNode, TrieValueAnalyzerCallback)}.
     */
    public void foundSmallerId(long ourId, long otherId);
  }
}
