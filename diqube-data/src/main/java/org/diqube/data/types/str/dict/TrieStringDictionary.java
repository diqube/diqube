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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrie;
import org.diqube.data.serialize.thrift.v1.SStringDictionaryTrieNode;
import org.diqube.data.types.str.dict.TrieValueAnalyzer.TrieValueAnalyzerCallback;

import com.google.common.collect.Sets;

/**
 * A {@link StringDictionary} which encodes the Strings as a Trie (prefix tree).
 * 
 * <p>
 * Accesses based on the ID of a value will be logarithmic, not constant.
 * 
 * <p>
 * The trie is made up of {@link TrieNode}s. There are two types of nodes, {@link ParentNode}s and {@link TerminalNode}
 * s. Each node represents a specific string, whcih can be constructed by appending all char[]s when walking dfrom the
 * root of the trie to the node.
 * 
 * <p>
 * The ParentNodes contain links to other nodes, where each link is based on a char[] denoting the additional characters
 * of the mapped strings. It can link to both types of nodes. The {@link TerminalNode}s in turn mark that a complete
 * value string is built when traversing the links from the root to that node. The {@link TerminalNode} then contains
 * the ID of the string as value.
 * 
 * <p>
 * The {@link ParentNode}s contain {@link ParentNode#getMinId()} and {@link ParentNode#getMaxId()} which contain the
 * terminal ID range that is mapped in the whole trie of that ParentNode. In addition to that there are two arrays: one
 * char[][] which contains the char[]s of the links of the trie; and a TrieNode[] which denotes the children (indices of
 * char[][] and TrieNode[] match, meaning if traversing into TrieNode[0] then char[0][] is added to the String value
 * being constructed). The char[][] is sorted in increasing order, there may be no overlapping entries. This means there
 * must not be an entry "a" and after that "ab", as then it is unclear what link to traverse. In that example case,
 * there should be a single "a" link, linking to another ParentNode which contains a "b".
 * 
 * <p>
 * Note that a {@link ParentNode} may contain a single "" (empty) entry which links to a {@link TerminalNode}, meaning
 * that the string that is referenced by the ParentNode is also a value string. This "" entry must be the first in the
 * char[][] and TrieNode[] (as they are sorted!) and must only point to a {@link TerminalNode}.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SStringDictionaryTrie.class)
public class TrieStringDictionary implements StringDictionary<SStringDictionaryTrie> {

  private ParentNode root;

  private String firstValue;
  private String lastValue;
  private long lastId;

  /** for deserialization */
  public TrieStringDictionary() {

  }

  /**
   * Create a new Trie dictionary.
   * 
   * @param trieRootNode
   *          The constructed trie, see class comment for details on that trie.
   * @param firstValue
   *          The smallest string value of the trie.
   * @param lastValue
   *          The biggest string value of the trie.
   * @param lastId
   *          The maximum ID of the trie (= ID of lastValue).
   */
  public TrieStringDictionary(ParentNode trieRootNode, String firstValue, String lastValue, long lastId) {
    root = trieRootNode;
    this.firstValue = firstValue;
    this.lastValue = lastValue;
    this.lastId = lastId;
  }

  @Override
  public Long getMaxId() {
    return lastId;
  }

  @Override
  public String decompressValue(long id) throws IllegalArgumentException {
    List<char[]> traversedCharSequences = new LinkedList<>();

    TrieNode<?> curNode = root;
    if (root.getMinId() > id || root.getMaxId() < id)
      throw new IllegalArgumentException(
          "Id " + id + "out of range; available range " + root.getMinId() + "-" + root.getMaxId());

    Function<TrieNode<?>, Integer> nodeCheckFn = (node) -> {
      if (node instanceof TerminalNode) {
        return Long.compare(id, ((TerminalNode) node).getTerminalId());
      }

      ParentNode c = (ParentNode) node;
      if (c.getMinId() <= id && c.getMaxId() >= id)
        return 0;
      return Long.compare(id, c.getMinId());
    };

    do {
      if (curNode instanceof ParentNode) {
        ParentNode parent = (ParentNode) curNode;
        int lo = 0;
        int high = parent.getChildNodes().length - 1;
        int proceedIntoIdx = -1;
        if (nodeCheckFn.apply(parent.getChildNodes()[lo]) == 0)
          proceedIntoIdx = lo;
        else if (nodeCheckFn.apply(parent.getChildNodes()[high]) == 0)
          proceedIntoIdx = high;
        else {
          while (high - lo > 5) {
            int mid = lo + ((high - lo) / 2);
            int compareRes = nodeCheckFn.apply(parent.getChildNodes()[mid]);
            if (compareRes == 0) {
              proceedIntoIdx = mid;
              break;
            } else if (compareRes < 0)
              high = mid;
            else
              lo = mid;
          }
          if (proceedIntoIdx == -1) {
            for (int i = lo; i <= high; i++) {
              TrieNode<?> child = parent.getChildNodes()[i];
              int compareRes = nodeCheckFn.apply(child);
              if (compareRes == 0) {
                proceedIntoIdx = i;
                break;
              } else if (compareRes < 0)
                break;
            }
            if (proceedIntoIdx == -1)
              throw new IllegalArgumentException(
                  "ID " + id + " not found in dictionary with id range " + root.getMinId() + "-" + root.getMaxId());
          }
        }

        if (proceedIntoIdx != -1) {
          traversedCharSequences.add(parent.getChildChars()[proceedIntoIdx]);
          curNode = parent.getChildNodes()[proceedIntoIdx];
        }
      }
    } while (!(curNode instanceof TerminalNode));

    StringBuilder sb = new StringBuilder();
    for (char[] seq : traversedCharSequences)
      sb.append(seq);

    return sb.toString();
  }

  @Override
  public String[] decompressValues(Long[] id) throws IllegalArgumentException {
    String[] res = new String[id.length];
    // TODO if IDs are sorted, we could speed this up...
    for (int i = 0; i < id.length; i++)
      res[i] = decompressValue(id[i]);

    return res;
  }

  @Override
  public long findIdOfValue(String value) throws IllegalArgumentException {
    long id = TrieUtil.findIdOfValue(value.toCharArray(), 0, root);
    if (id < 0)
      throw new IllegalArgumentException("Value '" + value + "' not available!");
    return id;
  }

  @Override
  public Long[] findIdsOfValues(String[] sortedValues) {
    // TODO #6 speed this up
    Long[] res = new Long[sortedValues.length];

    for (int i = 0; i < sortedValues.length; i++) {
      res[i] = TrieUtil.findIdOfValue(sortedValues[i].toCharArray(), 0, root);
      if (res[i] < 0)
        res[i] = -1L;
    }

    return res;
  }

  @Override
  public Long findGtEqIdOfValue(String value) {
    long id = TrieUtil.findIdOfValue(value.toCharArray(), 0, root);

    if (id >= 0)
      return id;

    long insertionPoint = -(id + 1);

    if (insertionPoint > lastId)
      return null;

    return id;
  }

  @Override
  public Long findLtEqIdOfValue(String value) {
    long id = TrieUtil.findIdOfValue(value.toCharArray(), 0, root);

    if (id >= 0)
      return id;

    long insertionPoint = -(id + 1);

    if (insertionPoint == 0L)
      return null;

    long idxOfNextSmallerValue = insertionPoint - 1;
    return -(idxOfNextSmallerValue + 1);
  }

  @Override
  public boolean containsAnyValue(String[] sortedValues) {
    // TODO #6 speed this up
    for (int i = 0; i < sortedValues.length; i++)
      if (TrieUtil.findIdOfValue(sortedValues[i].toCharArray(), 0, root) >= 0)
        return true;

    return false;
  }

  @Override
  public boolean containsAnyValueGtEq(String value) {
    int compareRes = lastValue.compareTo(value);
    return compareRes > 0 || compareRes == 0;
  }

  @Override
  public boolean containsAnyValueGt(String value) {
    int compareRes = lastValue.compareTo(value);
    return compareRes > 0;
  }

  @Override
  public boolean containsAnyValueLtEq(String value) {
    int compareRes = firstValue.compareTo(value);
    return compareRes < 0 || compareRes == 0;
  }

  @Override
  public boolean containsAnyValueLt(String value) {
    int compareRes = firstValue.compareTo(value);
    return compareRes < 0;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(String value) {
    Long gtEq = findGtEqIdOfValue(value);
    if (gtEq == null)
      return new HashSet<>();
    if (gtEq < 0)
      gtEq = -(gtEq + 1);
    return LongStream.rangeClosed(gtEq, lastId).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesGt(String value) {
    Long gtEq = findGtEqIdOfValue(value);
    if (gtEq == null)
      return new HashSet<>();
    if (gtEq < 0)
      gtEq = -(gtEq + 1);
    else {
      // gtEq found an equal id, increase it.
      gtEq++;
      if (gtEq > lastId)
        return new HashSet<>();
    }
    return LongStream.rangeClosed(gtEq, lastId).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesLt(String value) {
    Long ltEq = findLtEqIdOfValue(value);
    if (ltEq == null)
      return new HashSet<>();
    if (ltEq < 0)
      ltEq = -(ltEq + 1);
    else {
      // ltEq found an equal id, decrease it.
      ltEq--;
      if (ltEq < 0)
        return new HashSet<>();
    }
    return LongStream.rangeClosed(0, ltEq).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(String value) {
    Long ltEq = findLtEqIdOfValue(value);
    if (ltEq == null)
      return new HashSet<>();
    if (ltEq < 0)
      ltEq = -(ltEq + 1);
    return LongStream.rangeClosed(0, ltEq).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<String> otherDict) {
    // TODO #4 cache the results of the comparison calls.
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof TrieStringDictionary) {
      new TrieValueAnalyzer().analyzeTries(root, ((TrieStringDictionary) otherDict).root,
          new TrieValueAnalyzerCallback() {
            @Override
            public void foundEqualIds(long ourId, long otherId) {
              res.put(ourId, otherId);
            }

            @Override
            public void foundGreaterId(long ourId, long otherId) {
              // noop
            }

            @Override
            public void foundSmallerId(long ourId, long otherId) {
              // noop
            }
          });
    } else if (otherDict instanceof ConstantStringDictionary) {
      long otherId = ((ConstantStringDictionary) otherDict).getId();
      String otherValue = ((ConstantStringDictionary) otherDict).getValue();

      try {
        long ourId = findIdOfValue(otherValue);
        res.put(ourId, otherId);
      } catch (IllegalArgumentException e) {
        // swallow, return empty dict.
      }
    } else {
      // unfortunately no possibility to speed this up, so instantiate everything.
      // TODO this allows us to have only INT number of elements in dict. (no pressing problem, as ther's no
      // implementation which might land here currently)
      String[] values =
          decompressValues(LongStream.rangeClosed(0L, lastId).mapToObj(Long::valueOf).toArray(l -> new Long[l]));
      Long[] otherIds = otherDict.findIdsOfValues(values);
      for (int i = 0; i < values.length; i++) {
        if (otherIds[i] != -1L)
          res.put((long) i, otherIds[i]);
      }
    }
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<String> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();
    if (otherDict instanceof TrieStringDictionary) {
      Map<Long, Long> equal = new HashMap<>();
      Map<Long, Long> greater = new HashMap<>();
      Map<Long, Long> smaller = new HashMap<>();
      new TrieValueAnalyzer().analyzeTries(root, ((TrieStringDictionary) otherDict).root,
          new TrieValueAnalyzerCallback() {
            @Override
            public void foundEqualIds(long ourId, long otherId) {
              equal.put(ourId, otherId);
            }

            @Override
            public void foundGreaterId(long ourId, long otherId) {
              // ourId < otherId
              if (!greater.containsKey(ourId) || greater.get(ourId) > otherId)
                greater.put(ourId, otherId);
            }

            @Override
            public void foundSmallerId(long ourId, long otherId) {
              // ourId > otherId
              if (!smaller.containsKey(ourId) || smaller.get(ourId) < otherId)
                smaller.put(ourId, otherId);
            }
          });
      // deduce smaller IDs out of found greater IDs. As "greater" IDs are, if found, guaranteed to be the lowest IDs of
      // nodes that are greater, we can deduct the smaller ID here. This, of course, is only valid for nodes where the
      // "equal" method was not called, see below.
      for (Entry<Long, Long> greaterEntry : greater.entrySet()) {
        if (greaterEntry.getValue() <= 0)
          continue;
        long ourId = greaterEntry.getKey();
        long smallerId = greaterEntry.getValue() - 1;
        // make sure we can deduce - this is not the case if the new smallerId would be out of range.
        if (smallerId >= 0) {
          if (!smaller.containsKey(ourId) || smaller.get(ourId) < smallerId)
            smaller.put(ourId, smallerId);
        }
      }

      for (long ourId : Sets.union(equal.keySet(), smaller.keySet())) {
        if (equal.containsKey(ourId))
          res.put(ourId, equal.get(ourId));
        else if (smaller.containsKey(ourId))
          res.put(ourId, -(smaller.get(ourId) + 1));
      }
    } else if (otherDict instanceof ConstantStringDictionary) {
      long otherId = ((ConstantStringDictionary) otherDict).getId();
      String otherValue = ((ConstantStringDictionary) otherDict).getValue();

      Long ourGtEqId = findGtEqIdOfValue(otherValue);
      if (ourGtEqId != null) {
        if (ourGtEqId < 0) {
          ourGtEqId = -(ourGtEqId + 1);
          otherId = -(otherId + 1);
        }
        res.put(ourGtEqId, otherId);

        if (otherId > 0)
          otherId = -(otherId + 1);
        for (long ourId = ourGtEqId + 1; ourId <= lastId; ourId++)
          res.put(ourId, otherId);
      }
    } else {
      // Bad case: decompress whole array.
      String[] decompressedValues =
          decompressValues(LongStream.rangeClosed(0L, lastId).mapToObj(Long::valueOf).toArray(l -> new Long[l]));
      for (int i = 0; i < decompressedValues.length; i++) {
        Long otherId = otherDict.findGtEqIdOfValue(decompressedValues[i]);
        if (otherId == null)
          break;
        res.put((long) i, otherId);
      }
    }
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<String> otherDict) {
    // deduce result from >= comparison, as we cannot use TrieValueAnalyzer directly, as it has the weakest guarantees
    // on calling the "smaller" method.
    NavigableMap<Long, Long> res = findGtEqIds(otherDict);

    long firstOurIdGtEqHadValueOf;
    if (!res.isEmpty())
      firstOurIdGtEqHadValueOf = res.firstKey();
    else
      firstOurIdGtEqHadValueOf = lastId + 1;

    long otherMaxId = otherDict.getMaxId();
    for (Iterator<Entry<Long, Long>> it = res.entrySet().iterator(); it.hasNext();) {
      Entry<Long, Long> resEntry = it.next();
      if (resEntry.getValue() < 0) {
        long greatestSmallerId = -(resEntry.getValue() + 1);
        long smallestGreaterId = greatestSmallerId + 1;
        if (smallestGreaterId <= otherMaxId)
          resEntry.setValue(-(smallestGreaterId + 1));
        else
          // greater ID is not contained in otherDict, do not return that value.
          it.remove();
      }
    }

    // add entries for items of this map which were not returned by GtEqIds, because there is no >= inequality (but
    // then, there definitely is a <= inequality).
    for (long ourId = 0L; ourId < firstOurIdGtEqHadValueOf; ourId++)
      // all elements before the first returned GtEqId have a specific "largest smaller" otherId: 0.
      res.put(ourId, -(0 + 1L));

    return res;
  }

  @Override
  public void serialize(DataSerializationHelper mgr, SStringDictionaryTrie target) throws SerializationException {
    target.setFirstValue(firstValue);
    target.setLastValue(lastValue);
    target.setLastId(lastId);
    target.setRootNode(mgr.serializeChild(SStringDictionaryTrieNode.class, root));
  }

  @Override
  public void deserialize(DataSerializationHelper mgr, SStringDictionaryTrie source) throws DeserializationException {
    firstValue = source.getFirstValue();
    lastValue = source.getLastValue();
    lastId = source.getLastId();
    root = mgr.deserializeChild(ParentNode.class, source.getRootNode());
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this
        8 + // small fields
        firstValue.getBytes().length + //
        lastValue.getBytes().length + //
        root.calculateApproximateSizeInBytes();
  }

}
