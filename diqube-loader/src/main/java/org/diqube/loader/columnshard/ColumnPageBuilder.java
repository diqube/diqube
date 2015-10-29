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

import java.util.Map;
import java.util.NavigableMap;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.loader.compression.CompressedLongArrayBuilder;
import org.diqube.loader.compression.CompressedLongArrayBuilder.BitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongArrayBuilder.RunLengthAndBitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.util.Pair;

/**
 * Builds a {@link ColumnPage} and takes care of compressing the dictionary and the value array.
 *
 * @author Bastian Gloeckle
 */
public class ColumnPageBuilder {
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

    @SuppressWarnings("unchecked")
    CompressedLongArrayBuilder compressedBuilder = new CompressedLongArrayBuilder().withLogName(name).withValues(values)
        .withStrategies(BitEfficientCompressionStrategy.class, RunLengthAndBitEfficientCompressionStrategy.class);

    CompressedLongArray<?> compressedValues = compressedBuilder.build();

    // build final ColumnPage
    ColumnPage page = columnPageFactory.createDefaultColumnPage(columnPageDict, compressedValues, firstRowId, name);
    return page;

  }

}
