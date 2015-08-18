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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.diqube.data.lng.array.BitEfficientLongArray;
import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.lng.array.ReferenceBasedLongArray;
import org.diqube.data.lng.array.RunLengthLongArray;
import org.diqube.data.lng.array.TransitiveExplorableCompressedLongArray.TransitiveCompressionRatioCalculator;
import org.diqube.data.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.lng.dict.ConstantLongDictionary;
import org.diqube.data.lng.dict.EmptyLongDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds compressed {@link LongDictionary}s.
 * 
 * <p>
 * It currently builds {@link ArrayCompressedLongDictionary} objects with the better of two compressions:
 * 
 * <ul>
 * <li>A plain {@link BitEfficientLongArray}</li>
 * <li>A {@link ReferenceBasedLongArray} with a {@link BitEfficientLongArray} inside.</li>
 * </ul>
 * 
 * These compressions are fine to be used with dictionaries. {@link BitEfficientLongArray} use a special case for
 * {@link Long#MIN_VALUE} with which the {@link BitEfficientLongArray#get(int)} method might degenerate to a O(log m)
 * with m being the number of MIN_VALUES in the array. As dictionaries though have each value only once and therefore
 * {@link Long#MIN_VALUE} is contained in the array at most once, we can assume that log m is actually constant -
 * therefore the array-access complexity in both compression scenarios is constant. We therefore can hold that the
 * access to the dictionary itself is at most logarithmic. As counter example: It would not be meaningful to use
 * {@link RunLengthLongArray}s in dictionaries, as their get method is linear already and we would end up having worst
 * access to the dictionary of O(n log m).
 *
 * @author Bastian Gloeckle
 */
public class CompressedLongDictionaryBuilder {
  private static final Logger logger = LoggerFactory.getLogger(CompressedLongDictionaryBuilder.class);

  private static final List<LongArrayCompressionStrategy> COMPRESSION_STRATEGIES =
      Arrays.asList(new LongArrayCompressionStrategy[] { new BitEfficientCompressionStrategy(),
          new ReferenceAndBitEfficientCompressionStrategy() });

  private NavigableMap<Long, Long> entityMap;

  private String name;

  /**
   * @param entityMap
   *          From decompressed long value to temporary IDs that has been assigned already.
   */
  public CompressedLongDictionaryBuilder fromEntityMap(NavigableMap<Long, Long> entityMap) {
    this.entityMap = entityMap;
    return this;
  }

  public CompressedLongDictionaryBuilder withDictionaryName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Compresses the values and builds a new {@link LongDictionary} from them.
   * <p>
   * In addition to the new {@link LongDictionary}, this method returns a map that maps the temporary value IDs (as
   * provided in the map in {@link #fromEntityMap(NavigableMap)}) to the final IDs assigned by this builder. That map
   * will contain an entry only, if the ID of a specific value was actually changed by this builder.
   * 
   * @return A {@link Pair} of the newly built {@link LongDictionary} and the ID map (from temporary IDs to final IDs,
   *         containing the tuples where the ID was actually changed).
   */
  public Pair<LongDictionary<?>, Map<Long, Long>> build() {
    if (entityMap.size() == 0) {
      return new Pair<>(new EmptyLongDictionary(), new HashMap<>());
    } else if (entityMap.size() == 1) {
      Entry<Long, Long> entry = entityMap.entrySet().iterator().next();
      LongDictionary<?> dict = new ConstantLongDictionary(entry.getKey(), entry.getValue());
      return new Pair<>(dict, new HashMap<>());
    }

    Map<Long, Long> idMap = new HashMap<Long, Long>();

    long[] uncompressed = new long[entityMap.size()];
    Iterator<Entry<Long, Long>> entryIt = entityMap.entrySet().iterator();
    for (int i = 0; i < uncompressed.length; i++) {
      Entry<Long, Long> entry = entryIt.next();
      uncompressed[i] = entry.getKey();

      if (i != entry.getValue()) {
        idMap.put(entry.getValue(), (long) i);
      }
    }

    // Test all available compression strategies which one will compress the long array best, and then choose to use
    // that.
    LongArrayCompressionStrategy bestStrat = null;
    double bestRatio = 100.;
    for (LongArrayCompressionStrategy strat : COMPRESSION_STRATEGIES) {
      double ratio = strat.compressionRatio(uncompressed);

      logger.trace("Dictionary '{}' using {} would have expected ratio {}",
          new Object[] { name, strat.getClass().getSimpleName(), ratio });

      if (ratio < bestRatio) {
        bestRatio = ratio;
        bestStrat = strat;
      }
    }

    LongDictionary<?> dictRes = null;
    try {
      logger.debug("Compressing dictionary '{}' using {} (expected ratio {})",
          new Object[] { name, bestStrat.getClass().getSimpleName(), bestRatio });

      CompressedLongArray<?> compressedArray = bestStrat.compress(uncompressed);
      dictRes = new ArrayCompressedLongDictionary(compressedArray);
      return new Pair<>(dictRes, idMap);
    } finally {
      for (LongArrayCompressionStrategy strat : COMPRESSION_STRATEGIES)
        strat.clear();
    }
  }

  /**
   * A strategy on how to compress a sorted long array.
   * 
   * Calling the {@link #compressionRatio(long[])} method will tore thread local information, be sure to call
   * {@link #clear()} after you're done!
   */
  private interface LongArrayCompressionStrategy {
    /**
     * Returns a approx. compression ratio this strategy would achieve on the given sorted array. The array is expected
     * that it's values are pair-wise different.
     */
    double compressionRatio(long[] sortedInArray);

    /** Compress using this strategy. The array is expected that it's values are pair-wise different. */
    CompressedLongArray<?> compress(long[] sortedArray);

    /** Clear thread local information that was stored when calling {@link #compressionRatio(long[])}. */
    void clear();
  }

  /** Strategy using a plain {@link BitEfficientLongArray} */
  private static class BitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<BitEfficientLongArray> threadLocalArray = new ThreadLocal<>();

    @Override
    public double compressionRatio(long[] sortedInArray) {
      BitEfficientLongArray bitEfficient = new BitEfficientLongArray();
      threadLocalArray.set(bitEfficient);
      return bitEfficient.expectedCompressionRatio(sortedInArray, true);
    }

    @Override
    public CompressedLongArray<?> compress(long[] sortedArray) {
      BitEfficientLongArray be = threadLocalArray.get();
      if (be == null)
        be = new BitEfficientLongArray();
      else
        threadLocalArray.remove();

      be.compress(sortedArray, true);
      return be;
    }

    @Override
    public void clear() {
      if (threadLocalArray.get() != null)
        threadLocalArray.remove();
    }
  }

  /** Strategy using a {@link ReferenceBasedLongArray} with a {@link BitEfficientLongArray} inside. */
  private static class ReferenceAndBitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<ReferenceBasedLongArray> threadLocalArray = new ThreadLocal<>();

    @Override
    public double compressionRatio(long[] sortedInArray) {
      ReferenceBasedLongArray refBased = new ReferenceBasedLongArray();
      threadLocalArray.set(refBased);
      return refBased.expectedCompressionRatio(sortedInArray, true, new TransitiveCompressionRatioCalculator() {
        @Override
        public double calculateTransitiveCompressionRatio(long min, long secondMin, long max, long size) {
          int numberOfMinValues = 0;
          if (min == Long.MIN_VALUE) {
            numberOfMinValues = 1;
            min = secondMin;
          }
          return BitEfficientLongArray.calculateApproxCompressionRatio(min, max, (int) size, numberOfMinValues);
        }
      });
    }

    @Override
    public CompressedLongArray<?> compress(long[] sortedArray) {
      ReferenceBasedLongArray refBased = threadLocalArray.get();
      if (refBased == null)
        refBased = new ReferenceBasedLongArray();
      else
        threadLocalArray.remove();

      refBased.compress(sortedArray, true, () -> new BitEfficientLongArray());
      return refBased;
    }

    @Override
    public void clear() {
      if (threadLocalArray.get() != null)
        threadLocalArray.remove();
    }
  }

}
