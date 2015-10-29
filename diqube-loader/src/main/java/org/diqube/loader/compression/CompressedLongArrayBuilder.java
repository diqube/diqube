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

import java.util.ArrayList;
import java.util.List;

import org.diqube.data.types.lng.array.BitEfficientLongArray;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.array.ReferenceBasedLongArray;
import org.diqube.data.types.lng.array.RunLengthLongArray;
import org.diqube.data.types.lng.array.TransitiveExplorableCompressedLongArray.TransitiveCompressionRatioCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder of {@link CompressedLongArray}s based on various strategies.
 *
 * @author Bastian Gloeckle
 */
public class CompressedLongArrayBuilder {
  private static final Logger logger = LoggerFactory.getLogger(CompressedLongArrayBuilder.class);

  private long[] values;
  private Class<? extends LongArrayCompressionStrategy>[] strategyClasses;

  private String logName;

  public CompressedLongArrayBuilder withValues(long[] values) {
    this.values = values;
    return this;
  }

  @SuppressWarnings("unchecked")
  public CompressedLongArrayBuilder withStrategies(Class<? extends LongArrayCompressionStrategy>... strategyClasses) {
    this.strategyClasses = strategyClasses;
    return this;
  }

  public CompressedLongArrayBuilder withLogName(String logName) {
    this.logName = logName;
    return this;
  }

  public CompressedLongArray<?> build() {
    List<LongArrayCompressionStrategy> strategies = new ArrayList<>();
    for (Class<? extends LongArrayCompressionStrategy> strategyClass : strategyClasses)
      try {
        strategies.add(strategyClass.newInstance());
      } catch (InstantiationException | IllegalAccessException e) {
        // should never happen!
        throw new RuntimeException("Could not instantiate strategy class", e);
      }

    double bestRatio = 100.;
    LongArrayCompressionStrategy bestStrat = null;

    try {
      for (LongArrayCompressionStrategy strat : strategies) {
        double ratio = strat.compressionRatio(values);
        logger.trace("Values of '{}' using {} would have expected ratio {}",
            new Object[] { logName, strat.getClass().getSimpleName(), ratio });

        if (ratio < bestRatio) {
          bestRatio = ratio;
          bestStrat = strat;
        }
      }

      logger.debug("Compressing values of '{}' using {} (expected ratio {})",
          new Object[] { logName, bestStrat.getClass().getSimpleName(), bestRatio });

      CompressedLongArray<?> compressedValues = bestStrat.compress(values);

      return compressedValues;
    } finally {
      for (LongArrayCompressionStrategy strat : strategies)
        strat.clear();
    }
  }

  /**
   * A strategy on how to compress a unsorted long array.
   * 
   * Calling the {@link #compressionRatio(long[])} method will store thread local information, be sure to call
   * {@link #clear()} after you're done!
   */
  public static interface LongArrayCompressionStrategy {
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
  public static class BitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<BitEfficientLongArray> threadLocalArray = new ThreadLocal<>();

    /* package */ BitEfficientCompressionStrategy() {

    }

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
  public static class RunLengthAndBitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<RunLengthLongArray> threadLocalArray = new ThreadLocal<>();

    /* package */ RunLengthAndBitEfficientCompressionStrategy() {

    }

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

  /** Strategy using a {@link ReferenceBasedLongArray} with a {@link BitEfficientLongArray} inside. */
  public static class ReferenceAndBitEfficientCompressionStrategy implements LongArrayCompressionStrategy {

    private ThreadLocal<ReferenceBasedLongArray> threadLocalArray = new ThreadLocal<>();

    /* package */ ReferenceAndBitEfficientCompressionStrategy() {

    }

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
