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
package org.diqube.data.colshard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.ColumnType;
import org.diqube.data.Dictionary;
import org.diqube.util.DiqubeCollectors;
import org.diqube.util.Pair;

/**
 * Abstract implementation of a {@link StandardColumnShard}.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractStandardColumnShard implements StandardColumnShard {
  protected Dictionary<?> columnShardDictionary;

  protected String name;

  protected ColumnType columnType;

  /**
   * Map from firstId to {@link ColumnPage}
   */
  protected NavigableMap<Long, ColumnPage> pages;

  /**
   * Create a new column shard.
   * 
   * @param columnType
   *          Type of the column
   * @param name
   *          Name of the column
   * @param pages
   *          ColumnPages, mapping from first RowID to the page itself. This map object can be empty when creating this
   *          ColumnShard and be filled later on.
   * @param columnShardDictionary
   *          The Column dictionary, see class comment.
   */
  protected AbstractStandardColumnShard(ColumnType columnType, String name, NavigableMap<Long, ColumnPage> pages,
      Dictionary<?> columnShardDictionary) {
    this.columnType = columnType;
    this.name = name;
    this.pages = pages;
    this.columnShardDictionary = columnShardDictionary;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public NavigableMap<Long, ColumnPage> getPages() {
    return pages;
  }

  @Override
  public Dictionary<?> getColumnShardDictionary() {
    return columnShardDictionary;
  }

  @Override
  public ColumnType getColumnType() {
    return columnType;
  }

  @Override
  public long getNumberOfRowsInColumnShard() {
    if (pages.size() == 0)
      return 0;

    return pages.values().stream().mapToLong(page -> page.size()).sum();
  }

  @Override
  public long getFirstRowId() {
    return pages.firstKey();
  }

  @Override
  public Long[] resolveColumnValueIdsForRowsFlat(Long[] rowIds) {
    Map<Long, Long> rowIdToColumnValueId = resolveColumnValueIdsForRows(Arrays.asList(rowIds));
    Long[] res = new Long[rowIds.length];
    for (int i = 0; i < rowIds.length; i++)
      res[i] = (rowIdToColumnValueId.containsKey(rowIds[i])) ? rowIdToColumnValueId.get(rowIds[i]) : -1L;

    return res;
  }

  @Override
  public long resolveColumnValueIdForRow(Long rowId) {
    Entry<Long, ColumnPage> floorEntry = pages.floorEntry(rowId);
    if (floorEntry == null)
      return -1;
    ColumnPage page = floorEntry.getValue();
    if (rowId >= page.getFirstRowId() + page.getValues().size())
      return -1;

    return page.getColumnPageDict().decompressValue(page.getValues().get((int) (rowId - page.getFirstRowId())));
  }

  @Override
  public Map<Long, Long> resolveColumnValueIdsForRows(Collection<Long> rowIds) {
    Map<ColumnPage, NavigableSet<Long>> rowIdsByPage = rowIds.stream().parallel().collect( //
        Collectors.groupingByConcurrent(rowId -> {
          Entry<Long, ColumnPage> e = pages.floorEntry(rowId);
          if (e == null)
            // filter out too low row IDs. This might happen if this column shard has a firstRowId that is > than a
            // provided rowId.
            return null;
          return e.getValue();
        } , DiqubeCollectors.toNavigableSet()));
    Map<Long, Long> rowIdToColumnValueId = rowIdsByPage.entrySet().stream().parallel().filter(e -> e.getKey() != null)
        .flatMap(new Function<Entry<ColumnPage, NavigableSet<Long>>, Stream<Pair<Long, Long>>>() {
          @Override
          public Stream<Pair<Long, Long>> apply(Entry<ColumnPage, NavigableSet<Long>> entry) {
            List<Pair<Long, Long>> res = new ArrayList<>();

            ColumnPage page = entry.getKey();
            // take only those rowIDs that are inside the page - if there were row IDs provided that we do not have
            // any values of, do not return those entries. This may happen for the last page of a column.
            NavigableSet<Long> rowIds = entry.getValue().headSet(page.getFirstRowId() + page.getValues().size(), false);

            Long[] columnPageValueIds = new Long[rowIds.size()];
            Iterator<Long> rowIdIt = rowIds.iterator();
            for (int i = 0; i < columnPageValueIds.length; i++)
              // TODO #7 if long consecutive list of rowIds, we should fetch more elements here at once. Values could
              // be RLE encoded
              columnPageValueIds[i] = page.getValues().get((int) (rowIdIt.next() - page.getFirstRowId()));

            Long[] columnValueIds = page.getColumnPageDict().decompressValues(columnPageValueIds);

            rowIdIt = rowIds.iterator();
            for (int i = 0; i < columnPageValueIds.length; i++)
              res.add(new Pair<Long, Long>(rowIdIt.next(), columnValueIds[i]));

            return res.stream();
          }
        }).collect(() -> new HashMap<Long, Long>(), (map, pair) -> map.put(pair.getLeft(), pair.getRight()),
            (map1, map2) -> map1.putAll(map2));
    return rowIdToColumnValueId;
  }

  @Override
  public Set<Pair<Long, Integer>> getGoodResolutionPairs() {
    Set<Pair<Long, Integer>> res = pages.entrySet().stream(). //
        map(entry -> new Pair<Long, Integer>(entry.getKey(), entry.getValue().size())). //
        collect(Collectors.toSet());

    return res;
  }

}
