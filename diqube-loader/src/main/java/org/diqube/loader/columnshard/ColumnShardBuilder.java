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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnPageFactory;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.dict.DoubleDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.data.str.dict.StringDictionary;
import org.diqube.loader.Loader;
import org.diqube.loader.compression.CompressedDoubleDictionaryBuilder;
import org.diqube.loader.compression.CompressedLongDictionaryBuilder;
import org.diqube.loader.compression.CompressedStringDictionaryBuilder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds {@link ColumnShard}s and corresponding {@link ColumnPage}s.
 * 
 * <p>
 * Decompressed column values can be added to the column-that-will-be-built by simply calling the
 * {@link #addValues(Object[], Long)} method. As that method is thread-safe, this can be done in a multi-threaded way.
 * After all data was added, the {@link #build(Consumer)} method will build {@link ColumnShard} objects.
 *
 * <p>
 * Executing this Builder might take quite a lot of memory, as there is no notable compression used when creating the
 * column. In addition to that, all data of the column will be loaded in memory.
 *
 * <p>
 * The columns will statically be split up into pages of the length {@link #PROPOSAL_ROWS}.
 *
 * @author Bastian Gloeckle
 */
public class ColumnShardBuilder<T> {
  private static final Logger logger = LoggerFactory.getLogger(ColumnShardBuilder.class);
  public static final int PROPOSAL_ROWS = 50_000;

  /** Name of the Column to be created */
  private String name;

  /**
   * Holds all values of the column and maps each value to a value ID (see {@link #nextColumnDictId}).
   * 
   * This Map will be used to create the Column Dictionary when building the column.
   */
  private volatile ConcurrentNavigableMap<T, Long> columnDict = new ConcurrentSkipListMap<>();
  private AtomicLong nextColumnDictId = new AtomicLong(0);

  /**
   * The {@link ColumnPageProposal}s contain the value IDs (see {@link #columnDict}) of the values of the column.
   * 
   * Each {@link ColumnPageProposal} might contain up to {@link #PROPOSAL_ROWS} rows.
   * 
   * The {@link ColumnPageProposal} object that is at index A in this list contains the data of the row IDs
   * [A*PROPOSAL_ROWS..(A+1)*PROPOSAL_ROWS[.
   * 
   * These objects will be used to create the {@link ColumnPage}s later on.
   */
  private ArrayList<ColumnPageProposal> pageProposals = new ArrayList<>();

  private ColumnShardFactory columnShardFactory;
  private ColumnPageFactory columnPageFactory;

  private long firstRowIdInShard;

  /**
   * Build a new {@link ColumnShardBuilder}.
   * 
   * @param columnShardFactory
   *          A factory capable of creating a {@link ColumnShard} object.
   * @param columnPageFactory
   *          A factory capable of creating {@link ColumnPage} objects.
   * @param name
   *          The name of the column that should be created.
   * @param firstRowIdInShard
   *          The rowId of the first row the ColumnShard which is built by this builder should have. See JavaDoc of
   *          {@link Loader} for more info.
   */
  public ColumnShardBuilder(ColumnShardFactory columnShardFactory, ColumnPageFactory columnPageFactory, String name,
      long firstRowIdInShard) {
    this.columnShardFactory = columnShardFactory;
    this.columnPageFactory = columnPageFactory;
    this.name = name;
    this.firstRowIdInShard = firstRowIdInShard;
  }

  /**
   * Add new consecutive (uncompressed) values to this column.
   * 
   * <p>
   * The first value in the provided array will have the row ID provided in the firstValueRowId parameter. The second
   * value in the array will have firstValueRowId + 1 as row ID etc.
   * 
   * <p>
   * This method is thread safe.
   * 
   * <p>
   * This Builder expects that the list of rowIDs of all values added is consecutive after all data has been added using
   * this method. In the resulting ColumnShard/TableShard the lowest row ID (which is also used in this method call)
   * needs to be equal to the "first row ID" parameter specified in the constructor.
   * 
   * @param values
   *          The uncompressed values to be added to the column.
   * @param firstValueRowId
   *          The resulting Row ID of values[0], values[1] will be firstValueRowId + 1 etc.
   */
  public void addValues(T[] values, Long firstValueRowId) {
    // Add values to columnDict if needed, transform all values to column value IDs
    // Be aware that this here is a sequential stream! If using a parallel one, this method tries to acquire the same
    // few threads of the common ForkJoin thread pool, which the Parser might already use. We might then endup in a
    // somewhat deadlock situation.
    // TODO #47 rework the parallel streams architecture.
    long[] valueIds = Arrays.stream(values).sequential().mapToLong(new ToLongFunction<T>() {
      @Override
      public long applyAsLong(T value) {
        Long id = columnDict.get(value);
        if (id != null)
          return id;

        synchronized (columnDict) {
          id = columnDict.get(value);
          if (id != null)
            return id;
          id = nextColumnDictId.getAndIncrement();
          columnDict.put(value, id);
          return id;
        }
      }
    }).toArray();

    addValueIds(valueIds, 0, firstValueRowId);
  }

  /**
   * After adding the values of the column this method builds actual {@link ColumnShard}s.
   *
   * <p>
   * This is *NOT* thread-safe.
   * 
   * @return The shard.
   */
  @SuppressWarnings("unchecked")
  public StandardColumnShard build() {
    T sampleColumnDictKey = columnDict.keySet().iterator().next();
    Class<?> columnValueClass = sampleColumnDictKey.getClass();

    // Build ColumnShard including the column dict
    StandardColumnShard res = null;
    NavigableMap<Long, ColumnPage> pages = new TreeMap<>();
    Map<Long, Long> idChangeMap = null; // != null if the IDs in the final column dict were changed compared to
                                        // columnDict.

    if (columnValueClass.equals(String.class)) {
      CompressedStringDictionaryBuilder builder = new CompressedStringDictionaryBuilder();
      builder.fromEntityMap((ConcurrentNavigableMap<String, Long>) columnDict);
      Pair<StringDictionary, Map<Long, Long>> builderRes = builder.build();

      StringDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardStringColumnShard(name, pages, columnShardDictionary);
    } else if (columnValueClass.equals(Long.class)) {
      CompressedLongDictionaryBuilder builder = new CompressedLongDictionaryBuilder();
      builder.withDictionaryName(name).fromEntityMap((ConcurrentNavigableMap<Long, Long>) columnDict);
      Pair<LongDictionary, Map<Long, Long>> builderRes = builder.build();

      LongDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardLongColumnShard(name, pages, columnShardDictionary);
    } else if (columnValueClass.equals(Double.class)) {
      CompressedDoubleDictionaryBuilder builder = new CompressedDoubleDictionaryBuilder();
      builder.fromEntityMap((ConcurrentNavigableMap<Double, Long>) columnDict);
      Pair<DoubleDictionary, Map<Long, Long>> builderRes = builder.build();

      DoubleDictionary columnShardDictionary = builderRes.getLeft();
      idChangeMap = builderRes.getRight();

      res = columnShardFactory.createStandardDoubleColumnShard(name, pages, columnShardDictionary);
    } else {
      throw new UnsupportedOperationException("Only building of string, long and double dicts is implemented!");
    }

    // Prepare column builders
    List<ColumnPageBuilder> columnPageBuilders = new ArrayList<ColumnPageBuilder>(pageProposals.size());

    for (ColumnPageProposal proposal : pageProposals) {
      NavigableMap<Long, Long> valueToId = new TreeMap<>();

      int valueLength = proposal.calculateValueLength();
      long[] pageValue = new long[valueLength];
      long nextPageValueId = 0;

      for (int i = 0; i < valueLength; i++) {
        long value = proposal.valueIds[i];

        // Adjust ID that was stored in columnDict, if it has been adjusted when building the column dictionary above.
        if (idChangeMap != null && idChangeMap.containsKey(value))
          value = idChangeMap.get(value);

        // give this value a new ID which is valid for this column page
        if (!valueToId.containsKey(value)) {
          valueToId.put(value, nextPageValueId++);
        }

        // remember the new ID as value
        pageValue[i] = valueToId.get(value);
      }

      ColumnPageBuilder pageBuilder = new ColumnPageBuilder(columnPageFactory);
      pageBuilder.withFirstRowId(proposal.firstRowId).withValueMap(valueToId).withValues(pageValue)
          .withColumnPageName(name + "#" + proposal.firstRowId);

      columnPageBuilders.add(pageBuilder);
    }

    // build columns in parallel
    columnPageBuilders.stream().parallel().map(pageBuilder -> pageBuilder.build()).forEach(new Consumer<ColumnPage>() {
      @Override
      public void accept(ColumnPage page) {
        synchronized (pages) {
          // Add newly created page to the map that has already been provided to the ColumnShard.
          pages.put(page.getFirstRowId(), page);
        }
      }
    });

    return res;
  }

  /**
   * Adds the Value IDs as defined by {@link #columnDict} to the corresponding {@link ColumnPageProposal}s.
   * 
   * <p>
   * This method is thread-safe.
   * 
   * @param valueIds
   *          The IDs (defined in {@link #columnDict}) to be added.
   * @param startIdx
   *          Ignore all valueIds provided in the previous parameter that have index 0..startIdx-1.
   * @param firstValueRowId
   *          The row ID of the first value ID to be added (which is values[startIdx]).
   */
  private void addValueIds(long[] valueIds, int startIdx, long firstValueRowId) {
    long firstValueRowIndex = firstValueRowId - firstRowIdInShard;

    int pageId = (int) Math.floorDiv(firstValueRowIndex, PROPOSAL_ROWS);

    // check if there are too much values.
    int firstIdxInPage = (int) (firstValueRowIndex - (pageId * PROPOSAL_ROWS));
    int lengthStoredInThisPage = valueIds.length - startIdx;
    if (firstIdxInPage + lengthStoredInThisPage > PROPOSAL_ROWS) {
      // Not enough space in this page.
      lengthStoredInThisPage = PROPOSAL_ROWS - firstIdxInPage;
      // Descent recursively before enlarging pageProposals (see below) -> we will
      // enlarge pageProposals wide enough in the leaf executions of this recursion.
      addValueIds(valueIds, startIdx + lengthStoredInThisPage, firstValueRowId + lengthStoredInThisPage);
    }

    // ensure there's a ColumnPageProposal object available in pageProposals at index pageId.
    if (pageProposals.size() < pageId + 1 || pageProposals.get(pageId) == null) {
      synchronized (pageProposals) {
        if (pageProposals.size() < pageId + 1)
          pageProposals.addAll(Arrays.asList(new ColumnPageProposal[pageId + 1 - pageProposals.size()]));
        if (pageProposals.get(pageId) == null)
          pageProposals.set(pageId, new ColumnPageProposal(firstRowIdInShard + pageId * PROPOSAL_ROWS));
      }
    }

    // copy the value IDs into the PageProposal array at the correct indices. We will not interfere with other threads
    // here.
    System.arraycopy(valueIds, startIdx, pageProposals.get(pageId).valueIds, firstIdxInPage, lengthStoredInThisPage);
  }

  /**
   * Helper class that stores the values of this column for specific rows. It holds up to PROPOSAL_ROWS rows, starting
   * with row ID firstId.
   */
  private static class ColumnPageProposal {
    private static final long EMPTY = -1L;

    long firstRowId;
    long[] valueIds = new long[PROPOSAL_ROWS];

    public ColumnPageProposal(long firstRowId) {
      Arrays.fill(valueIds, EMPTY);
      this.firstRowId = firstRowId;
    }

    /**
     * Logarithmic calculation of the number of values that are available in the values array.
     * 
     * @return the number of elements available in {@link #valueIds}.
     */
    public int calculateValueLength() {
      if (valueIds.length == 0)
        return 0;
      if (valueIds[valueIds.length - 1] != EMPTY)
        return valueIds.length;
      if (valueIds[0] == EMPTY)
        return 0;

      int low = 0;
      int high = valueIds.length - 1;
      while (true) {
        int mid = low + ((high - low) >> 1);
        if (valueIds[mid] == EMPTY) {
          if (valueIds[mid - 1] != EMPTY)
            return mid;
          else {
            high = mid;
          }
        } else {
          if (valueIds[mid + 1] == EMPTY)
            return mid + 1;
          else {
            low = mid;
          }
        }
      }
    }
  }
}
