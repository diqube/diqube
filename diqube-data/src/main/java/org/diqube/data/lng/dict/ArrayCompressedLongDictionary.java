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
package org.diqube.data.lng.dict;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.LongStream;

import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;
import org.diqube.data.serialize.thrift.v1.SLongDictionaryArray;

/**
 * A {@link LongDictionary} based on a {@link CompressedLongArray}.
 *
 * TODO #3 base this dict on an array that can be indexed with long instead of int.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SLongDictionaryArray.class)
public class ArrayCompressedLongDictionary implements LongDictionary<SLongDictionaryArray> {

  private CompressedLongArray<?> sortedValues;

  /** for deserialization */
  public ArrayCompressedLongDictionary() {

  }

  /**
   * Create a new object.
   * 
   * @param sortedValues
   *          The values in a sorted array. The index of a value in the array will be its 'id' used by the dictionary.
   *          The array is expected to not have the same value twice, but it's values to be pair-wise different.
   */
  public ArrayCompressedLongDictionary(CompressedLongArray<?> sortedValues) {
    this.sortedValues = sortedValues;
  }

  @Override
  public Long getMaxId() {
    return (long) (sortedValues.size() - 1);
  }

  @Override
  public Long decompressValue(long id) throws IllegalArgumentException {
    if (id < 0 || id >= sortedValues.size())
      throw new IllegalArgumentException(
          "ID out of range to access dictionary: " + id + " (valid 0-" + (sortedValues.size() - 1) + ")");
    return sortedValues.get((int) id);
  }

  @Override
  public Long[] decompressValues(Long[] id) throws IllegalArgumentException {
    Long[] res = new Long[id.length];
    for (int i = 0; i < res.length; i++)
      res[i] = decompressValue(id[i]);
    return res;
  }

  @Override
  public long findIdOfValue(Long value) throws IllegalArgumentException {
    if (sortedValues.size() == 0)
      throw new IllegalArgumentException("Dictionary is empty.");

    int idx = findIndex(value, 0, sortedValues.size() - 1);
    if (idx < 0)
      throw new IllegalArgumentException("Value " + value + " could not be found in dictionary.");
    return idx;
  }

  @Override
  public Long findGtEqIdOfValue(Long value) {
    if (sortedValues.size() == 0)
      return null;

    int idx = findIndex(value, 0, sortedValues.size() - 1);
    if (idx >= 0)
      return (long) idx;
    if (idx == -(sortedValues.size() + 1))
      return null;
    return (long) idx;
  }

  @Override
  public Long findLtEqIdOfValue(Long value) {
    if (sortedValues.size() == 0)
      return null;

    int idx = findIndex(value, 0, sortedValues.size() - 1);
    if (idx >= 0)
      return (long) idx;

    long insertionPoint = -(idx + 1);
    if (insertionPoint == 0)
      return null;

    long idxOfNextSmallerValue = insertionPoint - 1;
    return -(idxOfNextSmallerValue + 1);
  }

  /**
   * Finds index of value in specific range of {@link #sortedValues}.
   * 
   * @return Index >= 0 if found. If not found, the result is (-1-instertionpoint), means < 0 and it might be -(high+2).
   *         This is equal to the result of {@link Arrays#binarySearch(long[], long)}.
   */
  private int findIndex(long value, int lo, int high) {
    // TODO #5 instead of decompressing a lot of values here, we should compress the search value itself and then search
    // for it in the compressed space. This should be implemented in CompressedLongArray.
    long decompressed = sortedValues.get(lo);
    if (decompressed == value)
      return lo;
    if (decompressed > value)
      return -1;
    decompressed = sortedValues.get(high);
    if (decompressed == value)
      return high;
    if (decompressed < value)
      return -2 - high;
    while (true) {
      if (high - lo <= 10) {
        for (int i = lo + 1; i <= high - 1; i++) {
          decompressed = sortedValues.get(i);
          if (decompressed == value)
            return i;
          if (decompressed > value) {
            return -1 - i;
          }
        }
        return -1 - high;
      }
      int mid = lo + ((high - lo) / 2);
      long midValue = sortedValues.get(mid);
      if (midValue == value)
        return mid;
      if (midValue > value)
        high = mid;
      else
        lo = mid;
    }
  }

  @Override
  public Long[] findIdsOfValues(Long[] sortedSearchValues) {
    Long[] res = new Long[sortedSearchValues.length];
    for (int i = 0; i < res.length; i++) {
      int idx = findIndex(sortedSearchValues[i], 0, sortedValues.size() - 1);
      res[i] = (idx >= 0) ? idx : -1L;
    }
    return res;
  }

  @Override
  public boolean containsAnyValue(Long[] sortedSearchValues) {
    for (int i = 0; i < sortedSearchValues.length; i++)
      if (findIndex(sortedSearchValues[i], 0, sortedValues.size() - 1) >= 0)
        return true;
    return false;
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof ArrayCompressedLongDictionary) {
      CompressedLongArray<?> otherSortedValues = ((ArrayCompressedLongDictionary) otherDict).sortedValues;
      if (sortedValues.size() == 0 || otherSortedValues.size() == 0)
        return res;

      // good case: we can traverse the two arrays simultaneously and only need to store O(1) in memory.
      int posThis = 0;
      int posOther = 0;
      long decompressedThis = sortedValues.get(posThis);
      long decompressedOther = otherSortedValues.get(posOther);
      while (posThis < sortedValues.size() && posOther < otherSortedValues.size()) {
        // move 'posThis' right until decompressedThis is >= decompressedOther
        while (posThis < sortedValues.size() - 1 && decompressedThis < decompressedOther)
          decompressedThis = sortedValues.get(++posThis);

        // move 'posOther' right until decompressedOther is >= decompressedThis
        while (posOther < otherSortedValues.size() - 1 && decompressedOther < decompressedThis)
          decompressedOther = otherSortedValues.get(++posOther);

        // validate if we have a match
        if (decompressedThis == decompressedOther) {
          res.put((long) posThis++, (long) posOther++);
          if (posThis < sortedValues.size() && posOther < otherSortedValues.size()) {
            decompressedThis = sortedValues.get(posThis);
            decompressedOther = otherSortedValues.get(posOther);
          }
        } else if ((posThis == sortedValues.size() - 1 && decompressedThis < decompressedOther)
            || (posThis == sortedValues.size() - 1 && posOther == otherSortedValues.size() - 1))
          break;
      }
    } else if (otherDict instanceof ConstantLongDictionary) {
      long otherId = ((ConstantLongDictionary) otherDict).getId();
      long otherValue = ((ConstantLongDictionary) otherDict).getDecompressedValue();

      try {
        long ourId = findIdOfValue(otherValue);
        res.put(ourId, otherId);
      } catch (IllegalArgumentException e) {
        // swallow, return empty dict.
      }
    } else if (otherDict instanceof EmptyLongDictionary) {
      // noop.
    } else {
      // Bad case: decompress whole array (should not happen, though)
      long[] decompressedValues = sortedValues.decompressedArray();
      Long[] otherIds = otherDict
          .findIdsOfValues(LongStream.of(decompressedValues).mapToObj(Long::valueOf).toArray(l -> new Long[l]));
      for (int i = 0; i < decompressedValues.length; i++) {
        Long otherId = otherIds[i];
        if (otherId != -1L)
          res.put((long) i, otherId);
      }
    }

    return res;
  }

  @Override
  public boolean containsAnyValueGtEq(Long value) {
    return sortedValues.get(sortedValues.size() - 1) >= value;
  }

  @Override
  public boolean containsAnyValueGt(Long value) {
    return sortedValues.get(sortedValues.size() - 1) > value;
  }

  @Override
  public boolean containsAnyValueLtEq(Long value) {
    return sortedValues.get(0) <= value;
  }

  @Override
  public boolean containsAnyValueLt(Long value) {
    return sortedValues.get(0) < value;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Long value) {
    int firstIdx = findIndex(value, 0, sortedValues.size() - 1);
    if (firstIdx < 0)
      firstIdx = -firstIdx - 1;

    Set<Long> res = new HashSet<>();
    if (firstIdx < sortedValues.size()) {
      // TODO #6 use LongStream and findGtEqIdOfValue
      for (int i = 0; i < sortedValues.size() - firstIdx; i++)
        res.add((long) i + firstIdx);
    }
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Long value) {
    int firstIdx = findIndex(value, 0, sortedValues.size() - 1);
    if (firstIdx < 0)
      firstIdx = -firstIdx - 1;
    else
      firstIdx++; // findIndex found ==, we want > though.

    Set<Long> res = new HashSet<>();
    if (firstIdx < sortedValues.size()) {
      for (int i = 0; i < sortedValues.size() - firstIdx; i++)
        res.add((long) i + firstIdx);
    }
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Long value) {
    int lastIdx = findIndex(value, 0, sortedValues.size() - 1);
    if (lastIdx < 0)
      lastIdx = -lastIdx - 2;
    else
      lastIdx--; // findIndex found ==, we want < though.

    Set<Long> res = new HashSet<>();
    if (lastIdx >= 0) {
      for (int i = 0; i < lastIdx + 1; i++)
        res.add((long) i);
    }
    return res;
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Long value) {
    int lastIdx = findIndex(value, 0, sortedValues.size() - 1);
    if (lastIdx < 0)
      lastIdx = -lastIdx - 2;

    Set<Long> res = new HashSet<>();
    if (lastIdx >= 0) {
      for (int i = 0; i < lastIdx + 1; i++)
        res.add((long) i);
    }
    return res;
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof ArrayCompressedLongDictionary) {
      CompressedLongArray<?> otherSortedValues = ((ArrayCompressedLongDictionary) otherDict).sortedValues;
      if (sortedValues.size() == 0 || otherSortedValues.size() == 0)
        return res;

      // good case: we can traverse the two arrays simultaneously and only need to store O(1) in memory.
      int posThis = 0;
      int posOther = 0;
      long decompressedThis = sortedValues.get(posThis);
      long decompressedOther = otherSortedValues.get(posOther);

      while (decompressedThis < decompressedOther && posThis < sortedValues.size() - 1)
        decompressedThis = sortedValues.get(++posThis);

      boolean doBreak = false;
      while (!doBreak) {

        while (decompressedThis > decompressedOther) { // "inner while loop"
          if (posOther == otherSortedValues.size() - 1) {
            while (posThis < sortedValues.size())
              res.put((long) posThis++, (long) -(posOther + 1));

            doBreak = true;
            break;
          }
          decompressedOther = otherSortedValues.get(++posOther);
        }

        if (!doBreak) {
          if (decompressedThis == decompressedOther) {
            res.put((long) posThis, (long) posOther);
          } else {
            // we know: decompressedOther > decompressedThis
            // but: in previous run of "inner while loop", decompressedOther < decompressedThis.
            // so: mark posThis as being greater than the previous posOther, restore position in other dict and proceed
            // 'this' one forward. If in the next loop, decompressedThis is still >= the next item in other, then we
            // will visit this very same execution again right away.
            decompressedOther = otherSortedValues.get(--posOther);

            res.put((long) posThis, (long) -(posOther + 1));
          }

          if (posThis == sortedValues.size() - 1)
            doBreak = true;
          else
            decompressedThis = sortedValues.get(++posThis);
        }
      }

    } else if (otherDict instanceof ConstantLongDictionary) {
      long otherId = ((ConstantLongDictionary) otherDict).getId();
      long otherValue = ((ConstantLongDictionary) otherDict).getDecompressedValue();

      Long ourGtEqId = findGtEqIdOfValue(otherValue);
      if (ourGtEqId != null) {
        if (ourGtEqId < 0) {
          ourGtEqId = -(ourGtEqId + 1);
          otherId = -(otherId + 1);
        }
        res.put(ourGtEqId, otherId);

        if (otherId > 0)
          otherId = -(otherId + 1);
        for (long ourId = ourGtEqId + 1; ourId < sortedValues.size(); ourId++)
          res.put(ourId, otherId);
      }
    } else if (otherDict instanceof EmptyLongDictionary) {
      // noop.
    } else {
      // Bad case: decompress whole array.
      long[] decompressedValues = sortedValues.decompressedArray();
      for (int i = 0; i < decompressedValues.length; i++) {
        Long otherId = otherDict.findLtEqIdOfValue(decompressedValues[i]);
        if (otherId == null)
          break;
        res.put((long) i, otherId);
      }
    }

    return res;
  }

  @Override
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Long> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof ArrayCompressedLongDictionary) {
      CompressedLongArray<?> otherSortedValues = ((ArrayCompressedLongDictionary) otherDict).sortedValues;
      if (sortedValues.size() == 0 || otherSortedValues.size() == 0)
        return res;

      // good case: we can traverse the two arrays simultaneously and only need to store O(1) in memory.
      int posThis = 0;
      int posOther = 0;
      long decompressedThis = sortedValues.get(posThis);
      long decompressedOther = otherSortedValues.get(posOther);

      boolean doBreak = false;
      while (!doBreak) {

        while (decompressedThis > decompressedOther) {
          if (posOther == otherSortedValues.size() - 1) {
            doBreak = true;
            break;
          }
          decompressedOther = otherSortedValues.get(++posOther);
        }

        if (!doBreak) {
          if (decompressedThis == decompressedOther)
            res.put((long) posThis, (long) posOther);
          else
            // we know: decompressedThis < decompressedOther
            res.put((long) posThis, (long) -(posOther + 1));

          if (posThis == sortedValues.size() - 1)
            // done processing
            doBreak = true;
          else
            // move this one further
            decompressedThis = sortedValues.get(++posThis);
        }
      }
    } else if (otherDict instanceof ConstantLongDictionary) {
      long otherId = ((ConstantLongDictionary) otherDict).getId();
      long otherValue = ((ConstantLongDictionary) otherDict).getDecompressedValue();

      Long ourLtEqId = findLtEqIdOfValue(otherValue);
      if (ourLtEqId != null) {
        if (ourLtEqId < 0) {
          ourLtEqId = -(ourLtEqId + 1);
          otherId = -(otherId + 1);
        }
        res.put(ourLtEqId, otherId);

        if (otherId > 0)
          otherId = -(otherId + 1);
        for (long ourId = 0; ourId < ourLtEqId; ourId++)
          res.put(ourId, otherId);
      }
    } else if (otherDict instanceof EmptyLongDictionary) {
      // noop.
    } else {
      // Bad case: decompress whole array.
      long[] decompressedValues = sortedValues.decompressedArray();
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
  public void serialize(DataSerializationHelper mgr, SLongDictionaryArray target) throws SerializationException {
    target.setArr(mgr.serializeChild(SLongCompressedArray.class, sortedValues));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void deserialize(DataSerializationHelper mgr, SLongDictionaryArray source) throws DeserializationException {
    sortedValues = mgr.deserializeChild(CompressedLongArray.class, source.getArr());
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return 16 + // object header of this.
        sortedValues.calculateApproximateSizeInBytes();
  }

}
