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
package org.diqube.data.dbl.dict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.diqube.data.Dictionary;
import org.diqube.util.DiqubeCollectors;
import org.diqube.util.DoubleUtil;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * A {@link DoubleDictionary} that implements a compression algorithm inspired by the "FPC" algorithm by Martin
 * Burtscher and Paruj Ratanaworabhan from the paper "High Throughput Compression of Double-Precision Floating-Point
 * Data".
 * 
 * The actual compression is implemented in {@link FpcPage}, as we can decompress only linearily (that means we have to
 * decompress all values with index < i for decompressing the value at index i). We therefore split up the set of
 * doubles that should be represented in this dictionary into single pages, where each page holds the intermediate
 * parameters of the compression algorithms, so that we are capable of decompressing a single page linearily and do not
 * have to start decompressing at the actual index 0L.
 *
 * @author Bastian Gloeckle
 */
public class FpcDoubleDictionary implements DoubleDictionary {

  private NavigableMap<Long, FpcPage> pages;
  private double lowestValue;
  private double highestValue;
  private long highestId;

  public FpcDoubleDictionary(NavigableMap<Long, FpcPage> pages, double lowestValue, double highestValue) {
    this.pages = pages;
    this.lowestValue = lowestValue;
    this.highestValue = highestValue;
    this.highestId = pages.lastEntry().getKey() + pages.lastEntry().getValue().getSize() - 1;
  }

  @Override
  public Long getMaxId() {
    Entry<Long, FpcPage> last = pages.lastEntry();

    if (last == null)
      return null;

    return last.getKey() + last.getValue().getSize() - 1;
  }

  @Override
  public Double decompressValue(long id) throws IllegalArgumentException {
    if (id < 0 || id > highestId)
      throw new IllegalArgumentException("Id out of range. Requested " + id + " but available are " + highestId);
    Entry<Long, FpcPage> e = pages.floorEntry(id);

    return e.getValue().get((int) (id - e.getKey()));
  }

  @Override
  public Double[] decompressValues(Long[] id) throws IllegalArgumentException {
    // group Ids by FpcPage to query each page only once in order to reduce number of times we decompress the beginning
    // of pages.
    Map<Entry<Long, FpcPage>, NavigableSet<Long>> grouped = Stream.of(id).collect(Collectors.groupingBy(i -> {
      if (i < 0 || i > highestId)
        throw new IllegalArgumentException("Invalid ID requested: " + i + " max available: " + highestId);
      return pages.floorEntry(i);
    } , DiqubeCollectors.toNavigableSet()));

    Map<Long, Double> resMap = new HashMap<>();

    grouped.forEach(new BiConsumer<Entry<Long, FpcPage>, NavigableSet<Long>>() {
      @Override
      public void accept(Entry<Long, FpcPage> t, NavigableSet<Long> u) {
        double[] pageRes = t.getValue().get((int) (u.first() - t.getKey()), (int) (u.last() - t.getKey()));
        for (long requestedId : u)
          resMap.put(requestedId, pageRes[(int) (requestedId - u.first())]);
      }
    });

    Double[] res = new Double[id.length];
    for (int i = 0; i < res.length; i++)
      res[i] = resMap.get(id[i]);

    return res;
  }

  /**
   * Uses a somewhat binary-search to search the ID of the given value.
   * 
   * <p>
   * Executes a binary search to find the {@link FpcPage} that possibly contains the value and then uses
   * {@link FpcPage#findIndex(double)} to search that page linearily.
   * 
   * @param value
   *          The searched value.
   * @return Either a positive number in which case it is the ID of the entry whose value is <b>equal</b> to the
   *         searched value. If negative the value is not included in any page and the result is encoded as -(id + 1)
   *         whereas "id" is the insertion point of the value (= the ID of the next bigger value).
   */
  private long binarySearchIdOfValue(double value) {
    // Binary-search the FpcPage containing the value
    List<FpcPage> pagesList = new ArrayList<>(pages.values());

    FpcPage resPage = null;
    int lo = 0;
    if (DoubleUtil.equals(pagesList.get(lo).get(0), value))
      return 0L;
    if (pagesList.get(lo).get(0) > value)
      return -1L;
    int hi = pagesList.size() - 1;
    if (pagesList.get(pagesList.size() - 1).get(0) < value
        || DoubleUtil.equals(pagesList.get(pagesList.size() - 1).get(0), value))
      resPage = pages.lastEntry().getValue();
    while (resPage == null && hi - lo > 5) {
      int mid = lo + ((hi - lo) / 2);
      double midFirstValue = pagesList.get(mid).get(0);
      if (DoubleUtil.equals(midFirstValue, value))
        return pagesList.get(mid).getFirstId();
      double nextFirstValue = pagesList.get(mid + 1).get(0); // there is always one, because mid < this.highestId.
      if (DoubleUtil.equals(nextFirstValue, value))
        return pagesList.get(mid + 1).getFirstId();

      if (value > midFirstValue && value < nextFirstValue)
        resPage = pagesList.get(mid);
      else if (midFirstValue < value)
        lo = mid;
      else
        hi = mid;
    }

    if (resPage == null) {
      for (int i = lo; i < hi; i++) {
        double firstValue = pagesList.get(i).get(0);
        if (DoubleUtil.equals(firstValue, value))
          return pagesList.get(i).getFirstId();
        double nextFirstValue = pagesList.get(i + 1).get(0); // there is always one, because i < hi.
        if (DoubleUtil.equals(nextFirstValue, value))
          return pagesList.get(i + 1).getFirstId();

        if (value > firstValue && value < nextFirstValue) {
          resPage = pagesList.get(i);
          break;
        }
      }
      if (resPage == null)
        resPage = pagesList.get(hi);
    }

    int foundPageIdx = resPage.findIndex(value);
    if (foundPageIdx < 0)
      return foundPageIdx - resPage.getFirstId();

    return foundPageIdx + resPage.getFirstId();
  }

  @Override
  public long findIdOfValue(Double value) throws IllegalArgumentException {
    long res = binarySearchIdOfValue(value);
    if (res < 0)
      throw new IllegalArgumentException("Value " + value + " not contained in dictionary.");

    return res;
  }

  @Override
  public Long[] findIdsOfValues(Double[] sortedValues) {
    // TODO #6 optimize
    Long[] res = new Long[sortedValues.length];
    for (int i = 0; i < res.length; i++) {
      long searchRes = binarySearchIdOfValue(sortedValues[i]);
      res[i] = (searchRes >= 0) ? searchRes : -1L;
    }
    return res;
  }

  @Override
  public Long findGtEqIdOfValue(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0 && -(searchRes + 1) > highestId)
      return null;
    return searchRes;
  }

  @Override
  public Long findLtEqIdOfValue(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0) {
      searchRes = -(searchRes + 1);
      if (searchRes == 0)
        // insertion point would be 0, means "value" is smaller than first entry
        return null;
      // searchRes is "insertion point" (=ID of next /greater/ value). Take ID of next smaller value.
      searchRes--;
      searchRes = -(searchRes + 1);
    }
    return searchRes;
  }

  @Override
  public boolean containsAnyValue(Double[] sortedValues) {
    // TODO #6 optimize
    for (int i = 0; i < sortedValues.length; i++) {
      if (binarySearchIdOfValue(sortedValues[i]) >= 0)
        return true;
    }
    return false;
  }

  @Override
  public boolean containsAnyValueGtEq(Double value) {
    return value.compareTo(highestValue) < 0 || DoubleUtil.equals(highestValue, value);
  }

  @Override
  public boolean containsAnyValueGt(Double value) {
    return value.compareTo(highestValue) < 0;
  }

  @Override
  public boolean containsAnyValueLtEq(Double value) {
    return value.compareTo(lowestValue) > 0 || DoubleUtil.equals(lowestValue, value);
  }

  @Override
  public boolean containsAnyValueLt(Double value) {
    return value.compareTo(lowestValue) > 0;
  }

  @Override
  public Set<Long> findIdsOfValuesGtEq(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0)
      searchRes = -(searchRes + 1);

    if (searchRes > highestId)
      return new HashSet<>();

    return LongStream.rangeClosed(searchRes, highestId).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesGt(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0)
      searchRes = -(searchRes + 1);
    else
      searchRes++; // searchRes was ID of /equal/ value -> increase!

    if (searchRes > highestId)
      return new HashSet<>();

    return LongStream.rangeClosed(searchRes, highestId).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesLt(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0)
      searchRes = -(searchRes + 1) - 1;
    else
      searchRes--; // searchRes was ID of /equal/ value -> decrease!

    if (searchRes < 0)
      return new HashSet<>();

    return LongStream.rangeClosed(0L, searchRes).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public Set<Long> findIdsOfValuesLtEq(Double value) {
    long searchRes = binarySearchIdOfValue(value);
    if (searchRes < 0) {
      searchRes = -(searchRes + 1) - 1;
      if (searchRes <= 0)
        return new HashSet<>();
    }

    return LongStream.rangeClosed(0L, searchRes).mapToObj(Long::valueOf).collect(Collectors.toSet());
  }

  @Override
  public NavigableMap<Long, Long> findEqualIds(Dictionary<Double> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof FpcDoubleDictionary) {
      FpcDoubleDictionary fpcOther = (FpcDoubleDictionary) otherDict;

      if (fpcOther.pages.size() == 1 && fpcOther.pages.firstEntry().getValue().getSize() == 0)
        return res;

      iterateOverValues(fpcOther, new IterationCallback() {
        @Override
        public void foundEqualIds(Long ourId, Long otherId) {
          res.put(ourId, otherId);
        }

        @Override
        public void foundGreaterId(Long ourId, Long otherId) {
          // noop.
        }

        @Override
        public void foundSmallerId(Long ourId, Long otherId) {
          // noop.
        }
      });
    } else if (otherDict instanceof ConstantDoubleDictionary) {
      double otherValue = ((ConstantDoubleDictionary) otherDict).getValue();
      long ourId = binarySearchIdOfValue(otherValue);

      if (ourId >= 0)
        res.put(ourId, ((ConstantDoubleDictionary) otherDict).getId());
    } else {
      // bad case: decompress everything.
      for (FpcPage page : pages.values()) {
        double[] values = page.get(0, page.getSize() - 1);
        for (int i = 0; i < values.length; i++) {
          double value = values[i];
          try {
            long otherId = otherDict.findIdOfValue(value);
            res.put(page.getFirstId() + i, otherId);
          } catch (IllegalArgumentException e) {
            // swallow, value not available in otherDict.
          }
        }
      }
    }

    return res;
  }

  @Override
  public NavigableMap<Long, Long> findGtEqIds(Dictionary<Double> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof FpcDoubleDictionary) {
      FpcDoubleDictionary fpcOther = (FpcDoubleDictionary) otherDict;

      if (fpcOther.pages.size() == 1 && fpcOther.pages.firstEntry().getValue().getSize() == 0)
        return res;

      iterateOverValues(fpcOther, new IterationCallback() {
        @Override
        public void foundEqualIds(Long ourId, Long otherId) {
          res.put(ourId, otherId);
        }

        @Override
        public void foundGreaterId(Long ourId, Long otherId) {
          // noop.
        }

        @Override
        public void foundSmallerId(Long ourId, Long otherId) {
          if (!res.containsKey(ourId) || (res.get(ourId) < 0 && -(res.get(ourId) + 1) < otherId))
            res.put(ourId, -(otherId + 1));
        }
      });
    } else if (otherDict instanceof ConstantDoubleDictionary) {
      long otherId = ((ConstantDoubleDictionary) otherDict).getId();
      double otherValue = ((ConstantDoubleDictionary) otherDict).getValue();

      Long ourGtEqId = findGtEqIdOfValue(otherValue);
      if (ourGtEqId != null) {
        if (ourGtEqId < 0) {
          ourGtEqId = -(ourGtEqId + 1);
          otherId = -(otherId + 1);
        }
        res.put(ourGtEqId, otherId);

        if (otherId > 0)
          otherId = -(otherId + 1);

        Entry<Long, FpcPage> lastEntry = pages.lastEntry();
        long numberOfOurValues = lastEntry.getKey() + lastEntry.getValue().getSize();

        for (long ourId = ourGtEqId + 1; ourId < pages.firstKey() + numberOfOurValues; ourId++)
          res.put(ourId, otherId);
      }
    } else {
      // Bad case: decompress whole array.
      Double[] decompressedValues =
          decompressValues(LongStream.rangeClosed(0L, highestId).mapToObj(Long::valueOf).toArray(l -> new Long[l]));
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
  public NavigableMap<Long, Long> findLtEqIds(Dictionary<Double> otherDict) {
    NavigableMap<Long, Long> res = new TreeMap<>();

    if (otherDict instanceof FpcDoubleDictionary) {
      FpcDoubleDictionary fpcOther = (FpcDoubleDictionary) otherDict;

      if (fpcOther.pages.size() == 1 && fpcOther.pages.firstEntry().getValue().getSize() == 0)
        return res;

      iterateOverValues(fpcOther, new IterationCallback() {
        @Override
        public void foundEqualIds(Long ourId, Long otherId) {
          res.put(ourId, otherId);
        }

        @Override
        public void foundGreaterId(Long ourId, Long otherId) {
          if (!res.containsKey(ourId) || (res.get(ourId) < 0 && -(res.get(ourId) + 1) > otherId))
            res.put(ourId, -(otherId + 1));
        }

        @Override
        public void foundSmallerId(Long ourId, Long otherId) {
          // noop.
        }
      });
    } else if (otherDict instanceof ConstantDoubleDictionary) {
      long otherId = ((ConstantDoubleDictionary) otherDict).getId();
      double otherValue = ((ConstantDoubleDictionary) otherDict).getValue();

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
    } else {
      // Bad case: decompress whole array.
      Double[] decompressedValues =
          decompressValues(LongStream.rangeClosed(0L, highestId).mapToObj(Long::valueOf).toArray(l -> new Long[l]));
      for (int i = 0; i < decompressedValues.length; i++) {
        Long otherId = otherDict.findGtEqIdOfValue(decompressedValues[i]);
        if (otherId == null)
          break;
        res.put((long) i, otherId);
      }
    }

    return res;
  }

  /**
   * Iterates over this dicts values and the given other dicts values and identifies IDs where there are <, > and ==
   * relations.
   * 
   * @param callback
   *          The callback will be called for _all_ found relations of every item in this dict. This means that for each
   *          item in this dict, there will be at least once the {@link IterationCallback#foundGreaterId(Long, Long)}
   *          and {@link IterationCallback#foundSmallerId(Long, Long)} called (at least once with the smallest greater
   *          ID and the greatest smaller ID). Additionally, {@link IterationCallback#foundEqualIds(Long, Long)} will be
   *          called if a valid pair is found.
   */
  private void iterateOverValues(FpcDoubleDictionary fpcOther, IterationCallback callback) {
    PeekingIterator<Entry<Long, FpcPage>> otherIt = Iterators.peekingIterator(fpcOther.pages.entrySet().iterator());
    FpcPage otherFirstPage = otherIt.peek().getValue();
    double[] otherValues = otherFirstPage.get(0, otherFirstPage.getSize() - 1);

    int otherIdx = 0;
    for (Entry<Long, FpcPage> ourEntry : pages.entrySet()) {
      FpcPage ourPage = ourEntry.getValue();
      long ourFirstIdx = ourEntry.getKey();
      double[] ourValues = ourPage.get(0, ourPage.getSize() - 1);
      for (int i = 0; i < ourValues.length; i++) {
        double ourValue = ourValues[i];

        long otherFirstIdx = otherIt.peek().getKey();

        // move "otherIdx" to the right, until ourValue <= otherValues[otherNextIdx]
        while (ourValue > otherValues[otherIdx] && !DoubleUtil.equals(ourValue, otherValues[otherIdx])) {
          otherIdx++;

          if (otherIdx == otherValues.length) {
            // end of other page, move to next page.
            otherIt.next();
            if (!otherIt.hasNext()) {
              LongStream.rangeClosed(ourFirstIdx + i, highestId)
                  .forEach(ourId -> callback.foundSmallerId(ourId, fpcOther.highestId));
              return;
            }

            otherFirstIdx = otherIt.peek().getKey();
            otherIdx = 0;
            FpcPage newOtherPage = otherIt.peek().getValue();
            otherValues = newOtherPage.get(0, newOtherPage.getSize() - 1);
          }
        }

        if (DoubleUtil.equals(ourValue, otherValues[otherIdx])) {
          if (otherFirstIdx + otherIdx - 1 >= 0)
            callback.foundSmallerId(ourFirstIdx + i, otherFirstIdx + otherIdx - 1);

          callback.foundEqualIds(ourFirstIdx + i, otherFirstIdx + otherIdx);

          if (otherFirstIdx + otherIdx + 1 <= fpcOther.highestId)
            callback.foundGreaterId(ourFirstIdx + i, otherFirstIdx + otherIdx + 1);
        } else {
          // we know: ourValue < other value, but in the previous run of the while loop above, ourValue > other value.
          callback.foundGreaterId(ourFirstIdx + i, otherFirstIdx + otherIdx);

          if (otherFirstIdx + otherIdx - 1 >= 0)
            callback.foundSmallerId(ourFirstIdx + i, otherFirstIdx + otherIdx - 1);
        }
      }
    }
  }

  /**
   * Callback interface for {@link FpcDoubleDictionary#iterateOverValues(FpcDoubleDictionary, IterationCallback)}, see
   * that java doc.
   */
  private static interface IterationCallback {
    /**
     * IDs in this dict and the other dict have been found which have equal values.
     */
    public void foundEqualIds(Long ourId, Long otherId);

    /**
     * value of this dict at ourId is < than value of other dict at otherId.
     */
    public void foundGreaterId(Long ourId, Long otherId);

    /**
     * value of this dict at ourId is > than value of other dict at otherId.
     */
    public void foundSmallerId(Long ourId, Long otherId);
  }

}
