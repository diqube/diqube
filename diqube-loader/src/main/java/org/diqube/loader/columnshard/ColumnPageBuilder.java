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
package org.diqube.loader.columnshard;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnPageFactory;
import org.diqube.data.lng.array.BitEfficientLongArray;
import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.lng.array.RunLengthLongArray;
import org.diqube.data.lng.array.TransitiveExplorableCompressedLongArray.TransitiveCompressionRatioCalculator;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a {@link ColumnPage} and takes care of compressing the dictionary and the value array.
 *
 * @author Bastian Gloeckle
 */
public class ColumnPageBuilder {
  private static final Logger logger = LoggerFactory.getLogger(ColumnPageBuilder.class);

  private static final List<LongArrayCompressionStrategy> COMPRESSION_STRATEGIES =
      Arrays.asList(new LongArrayCompressionStrategy[] { new BitEfficientCompressionStrategy(),
          new RunLengthAndBitEfficientCompressionStrategy() });

  private long firstRowId;
  private NavigableMap<Long, Long> valueMap;
  private long[] values;
  private ColumnPageFactory columnPageFactory;

  private String name;

  public ColumnPageBuilder(ColumnPageFactory columnPageFactory) {
    this.columnPageFactory = columnPageFactory;
  }

  public ColumnPageBuilder withColumnPageName(String name) {
    this.name = name;
    return this;
  }

  /**
   * @param firstRowId
   *          The row ID of the first of the {@link #withValues(long[])}.
   */
  public ColumnPageBuilder withFirstRowId(long firstRowId) {
    this.firstRowId = firstRowId;
    return this;
  }

  /**
   * @param valueMap
   *          From value long to id long. ID longs are the same that are used in {@link #withValues(long[])}.
   */
  public ColumnPageBuilder withValueMap(NavigableMap<Long, Long> valueMap) {
    this.valueMap = valueMap;
    return this;
  }

  /**
   * @param values
   *          Value IDs for each row of the future column page. The Value IDs are the values of the map specified in
   *          {@link #withValueMap(NavigableMap)}. This array will be written to by this builder.
   */
  public ColumnPageBuilder withValues(long[] values) {
    this.values = values;
    return this;
  }

  /**
   * Build a new {@link ColumnPage} and take care of compression.
   * 
   * @return The new {@link ColumnPage}
   */
  public ColumnPage build() {
    // Build a LongDictionary from the values we want to store in the page. This might re-assign IDs.
    CompressedLongDictionaryBuilder columnPageDictBuilder = new CompressedLongDictionaryBuilder();
    columnPageDictBuilder.withDictionaryName(name).fromEntityMap(this.valueMap);
    Pair<LongDictionary<?>, Map<Long, Long>> builderRes = columnPageDictBuilder.build();

    LongDictionary<?> columnPageDict = builderRes.getLeft();
    Map<Long, Long> columnPageIdAdjust = builderRes.getRight();

    // If the builder of the columnPage dict decided to adjust the IDs, we need to integrate those changes into
    // pageValue array.
    if (columnPageIdAdjust != null) {
      for (int i = 0; i < values.length; i++) {
        if (columnPageIdAdjust.containsKey(values[i]))
          values[i] = columnPageIdAdjust.get(values[i]);
      }
    }

    double bestRatio = 100.;
    LongArrayCompressionStrategy bestStrat = null;

    for (LongArrayCompressionStrategy strat : COMPRESSION_STRATEGIES) {
      double ratio = strat.compressionRatio(values);
      logger.trace("Values of '{}' using {} would have expected ratio {}",
          new Object[] { name, strat.getClass().getSimpleName(), ratio });

      if (ratio < bestRatio) {
        bestRatio = ratio;
        bestStrat = strat;
      }
    }

    try {
      logger.debug("Compressing values of '{}' using {} (expected ratio {})",
          new Object[] { name, bestStrat.getClass().getSimpleName(), bestRatio });

      CompressedLongArray<?> compressedValues = bestStrat.compress(values);

      // build final ColumnPage
      ColumnPage page = columnPageFactory.createColumnPage(columnPageDict, compressedValues, firstRowId, name);
      return page;
    } finally {
      for (LongArrayCompressionStrategy strat : COMPRESSION_STRATEGIES)
        strat.clear();
    }

  }

  /**
   * A strategy on how to compress a unsorted long array.
   * 
   * Calling the {@link #compressionRatio(long[])} method will tore thread local information, be sure to call
   * {@link #clear()} after you're done!
   */
  private interface LongArrayCompressionStrategy {
    /**
     * Returns a approx. compression ratio this strategy would achieve on the given sorted array.
     */
    double compressionRatio(long[] array);

    /** Compress using this strategy. */
    CompressedLongArray<?> compress(long[] array);

    /** Clear thread local information that was stored when calling {@link #compressionRatio(long[])}. */
    void clear();
  }

  /** Strategy using a plain {@link BitEfficientLongArray} */
  private static class BitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<BitEfficientLongArray> threadLocalArray = new ThreadLocal<>();

    @Override
    public double compressionRatio(long[] array) {
      BitEfficientLongArray bitEfficient = new BitEfficientLongArray();
      threadLocalArray.set(bitEfficient);
      return bitEfficient.expectedCompressionRatio(array, false);
    }

    @Override
    public CompressedLongArray<?> compress(long[] array) {
      BitEfficientLongArray be = threadLocalArray.get();
      if (be == null)
        be = new BitEfficientLongArray();
      else
        threadLocalArray.remove();

      be.compress(array, false);
      return be;
    }

    @Override
    public void clear() {
      if (threadLocalArray.get() != null)
        threadLocalArray.remove();
    }
  }

  /** Strategy using a {@link RunLengthLongArray} with a {@link BitEfficientLongArray} inside. */
  private static class RunLengthAndBitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<RunLengthLongArray> threadLocalArray = new ThreadLocal<>();

    @Override
    public double compressionRatio(long[] array) {
      RunLengthLongArray runLength = new RunLengthLongArray();
      threadLocalArray.set(runLength);
      return runLength.expectedCompressionRatio(array, false, new TransitiveCompressionRatioCalculator() {
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
    public CompressedLongArray<?> compress(long[] array) {
      RunLengthLongArray refBased = threadLocalArray.get();
      if (refBased == null)
        refBased = new RunLengthLongArray();
      else
        threadLocalArray.remove();

      refBased.compress(array, false, () -> new BitEfficientLongArray());
      return refBased;
    }

    @Override
    public void clear() {
      if (threadLocalArray.get() != null)
        threadLocalArray.remove();
    }
  }
}
