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
package org.diqube.flatten;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.stream.LongStream;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.column.DefaultColumnPage;
import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedColumnPage;
import org.diqube.data.flatten.FlattenedDelegateLongDictionary;
import org.diqube.data.flatten.IndexFilteringCompressedLongArray;
import org.diqube.data.flatten.IndexRemovingCompressedLongArray;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.loader.columnshard.ColumnPageBuilder;
import org.diqube.loader.compression.CompressedLongArrayBuilder;
import org.diqube.loader.compression.CompressedLongArrayBuilder.BitEfficientCompressionStrategy;
import org.diqube.loader.compression.CompressedLongArrayBuilder.ReferenceAndBitEfficientCompressionStrategy;
import org.diqube.util.DiqubeCollectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * Builds {@link ColumnPage}s that are used for flattened column shards.
 * 
 * <p>
 * This can either be {@link FlattenedColumnPage}s or actual normal {@link ColumnPage}s at the discretion of this class.
 *
 * @author Bastian Gloeckle
 */
public class FlattenedColumnPageBuilder {
  private String colName;
  private long firstRowId;
  private ColumnPage delegate;
  private SortedSet<Long> notAvailableRowIds;
  private FlattenDataFactory flattenDataFactory;
  private ColumnPageFactory columnPageFactory;

  /* package */ FlattenedColumnPageBuilder(FlattenDataFactory flattenDataFactory, ColumnPageFactory columnPageFactory) {
    this.flattenDataFactory = flattenDataFactory;
    this.columnPageFactory = columnPageFactory;
  }

  /**
   * @param colName
   *          Name of column that this page will belong to.
   */
  public FlattenedColumnPageBuilder withColName(String colName) {
    this.colName = colName;
    return this;
  }

  /**
   * @param firstRowId
   *          First row id of new col page.
   */
  public FlattenedColumnPageBuilder withFirstRowId(long firstRowId) {
    this.firstRowId = firstRowId;
    return this;
  }

  /**
   * @param delegate
   *          {@link ColumnPage} whose values the new col page should return.
   */
  public FlattenedColumnPageBuilder withDelegate(ColumnPage delegate) {
    this.delegate = delegate;
    return this;
  }

  /**
   * @param notAvailableRowIds
   *          Row IDs (in the scope of the delegate {@link ColumnPage}) which should not be contained by the returned
   *          new {@link FlattenedColumnPage}.
   */
  public FlattenedColumnPageBuilder withNotAvailableRowIds(SortedSet<Long> notAvailableRowIds) {
    this.notAvailableRowIds = notAvailableRowIds;
    return this;
  }

  /**
   * Build and return a new {@link ColumnPage} that adheres to the settings in this builder.
   * 
   * <p>
   * The returned {@link ColumnPage} can be a {@link FlattenedColumnPage} or even a "normal" {@link DefaultColumnPage}
   * built by an internal {@link ColumnPageBuilder} - at the discretion of this builder.
   * 
   * <p>
   * Nevertheless what kind of {@link ColumnPage} is returned, the col page dict will definitely be a
   * {@link FlattenedDelegateLongDictionary}.
   * 
   * @return Either a new {@link ColumnPage} or <code>null</code> if the returned colPage would be empty.
   */
  public ColumnPage build() {
    if (delegate.getValues().size() == notAvailableRowIds.size())
      // we'd remove all values from the delegate, so we actually do not need to build a page at all.
      return null;

    ColumnPage newPage;

    // We use some heuristic here to produce some well-compressed output.
    if (notAvailableRowIds.size() <= (int) Math.ceil(delegate.getValues().size() / 3.)) {
      // we remove <= 1/3 of rows.

      // -> use delegate colPage that removes indices
      IndexRemovingCompressedLongArray values = flattenDataFactory.createIndexRemovingCompressedLongArray(
          delegate.getValues(), compressRowIdIndicesToIndicesArray(notAvailableRowIds, delegate.getFirstRowId()), 0L);

      newPage = flattenDataFactory.createFlattenedColumnPage(colName + "#" + firstRowId,
          flattenDataFactory.createFlattenedDelegateLongDictionary(delegate.getColumnPageDict()), delegate, values,
          firstRowId);
    } else if (notAvailableRowIds.size() >= 2 * (int) Math.ceil(delegate.getValues().size() / 3.)) {
      // we remove >= 2/3 of rows.

      // -> use delegate colPage that filters indices (= only returns those values at specific indices)
      NavigableSet<Long> availableRowIds =
          LongStream.range(delegate.getFirstRowId(), delegate.getFirstRowId() + delegate.getValues().size())
              .mapToObj(Long::valueOf).filter(l -> !notAvailableRowIds.contains(l))
              .collect(DiqubeCollectors.toNavigableSet());

      IndexFilteringCompressedLongArray values = flattenDataFactory.createIndexFilteringCompressedLongArray(
          delegate.getValues(), compressRowIdIndicesToIndicesArray(availableRowIds, delegate.getFirstRowId()), 0L);

      newPage = flattenDataFactory.createFlattenedColumnPage(colName + "#" + firstRowId,
          flattenDataFactory.createFlattenedDelegateLongDictionary(delegate.getColumnPageDict()), delegate, values,
          firstRowId);
    } else {
      // we remove x rows where 1/3 < x < 2/3. Let's re-encode the whole ColPage - as we assume that the values in the
      // col are better compressable than the index arrays in the other cases (those arrays may contain consecutive
      // increasing numbers which are not well compressable).

      ColumnPageBuilder colPageBuilder = new ColumnPageBuilder(columnPageFactory);
      colPageBuilder.withColumnPageName(colName + "#" + firstRowId).withFirstRowId(firstRowId);

      long[] newValueIds = new long[delegate.getValues().size() - notAvailableRowIds.size()];
      long[] oldValueIds = delegate.getValues().decompressedArray();
      NavigableMap<Long, Long> newValueMap = new TreeMap<>();
      int nextNewIdx = 0;
      PeekingIterator<Long> notAvailIt = Iterators.peekingIterator(notAvailableRowIds.iterator());
      for (int i = 0; i < oldValueIds.length; i++) {
        if (notAvailIt.hasNext() && notAvailIt.peek() == delegate.getFirstRowId() + i) {
          notAvailIt.next();
          continue;
        }

        newValueIds[nextNewIdx] = oldValueIds[i];
        newValueMap.put(delegate.getColumnPageDict().decompressValue(oldValueIds[i]), oldValueIds[i]);
        nextNewIdx++;
      }

      newPage = colPageBuilder.withValueMap(newValueMap).withValues(newValueIds).
          // facade the colPageDict with a delegate dict
          withColumnPageDictFunction(
              colPageDict -> flattenDataFactory.createFlattenedDelegateLongDictionary(colPageDict))
          .build();
    }

    return newPage;
  }

  /**
   * Transforms a set of rowIds which should not be accessible in {@link FlattenedColumnPage} to the
   * {@link CompressedLongArray} of "indices" of the values array of that page, as that is needed to construct a
   * {@link FlattenedColumnPage}.
   * 
   * @param rowIds
   *          the rowIds which should be converted to indices and then stored in a {@link CompressedLongArray}.
   * @param firstRowId
   *          The value to subtract from each of the rowIds.
   * @return
   */
  private CompressedLongArray<?> compressRowIdIndicesToIndicesArray(SortedSet<Long> rowIds, long firstRowId) {
    // do not use RunLengthLongArray strategy, as stated by FlattenedColumnPage
    @SuppressWarnings("unchecked")
    CompressedLongArrayBuilder builder = new CompressedLongArrayBuilder()
        .withStrategies(BitEfficientCompressionStrategy.class, ReferenceAndBitEfficientCompressionStrategy.class);

    long[] values = rowIds.stream().mapToLong(rowId -> rowId - firstRowId).toArray();

    builder.withValues(values);

    return builder.build();
  }
}
