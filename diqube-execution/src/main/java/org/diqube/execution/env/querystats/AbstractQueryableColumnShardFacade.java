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
package org.diqube.execution.env.querystats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.ColumnType;
import org.diqube.data.Dictionary;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.DiqubeCollectors;
import org.diqube.util.Pair;

/**
 * Abstract base implementation of {@link QueryableColumnShard}.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractQueryableColumnShardFacade implements QueryableColumnShard {

  private ColumnShard delegate;
  private QueryRegistry queryRegistry;
  private boolean isTempColumn;

  public AbstractQueryableColumnShardFacade(ColumnShard delegate, boolean isTempColumn, QueryRegistry queryRegistry) {
    this.delegate = delegate;
    this.isTempColumn = isTempColumn;
    this.queryRegistry = queryRegistry;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return delegate.calculateApproximateSizeInBytes() + //
        9 // some constant for the other fields.
        ;
  }

  @Override
  public Dictionary<?> getColumnShardDictionary() {
    return delegate.getColumnShardDictionary();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public ColumnType getColumnType() {
    return delegate.getColumnType();
  }

  @Override
  public long getFirstRowId() {
    return delegate.getFirstRowId();
  }

  @Override
  public Map<Long, Long> resolveColumnValueIdsForRows(Collection<Long> rowIds) {
    if (delegate instanceof StandardColumnShard) {
      QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();

      try {
        Map<ColumnPage, NavigableSet<Long>> rowIdsByPage = rowIds.stream().parallel().collect( //
            Collectors.groupingByConcurrent(rowId -> {
              QueryUuid.setCurrentThreadState(uuidState);
              try {
                Entry<Long, ColumnPage> e = ((StandardColumnShard) delegate).getPages().floorEntry(rowId);
                if (e == null)
                  // filter out too low row IDs. This might happen if this column shard has a firstRowId that is > than
                  // a provided rowId.
                  return null;
                return e.getValue();
              } finally {
                QueryUuid.clearCurrent();
              }
            } , DiqubeCollectors.toNavigableSet()));

        QueryUuid.setCurrentThreadState(uuidState);
        for (ColumnPage page : rowIdsByPage.keySet())
          queryRegistry.getOrCreateCurrentStatsManager().registerPageAccess(page, isTempColumn);

        Map<Long, Long> rowIdToColumnValueId =
            rowIdsByPage.entrySet().stream().parallel().filter(e -> e.getKey() != null)
                .flatMap(new Function<Entry<ColumnPage, NavigableSet<Long>>, Stream<Pair<Long, Long>>>() {
                  @Override
                  public Stream<Pair<Long, Long>> apply(Entry<ColumnPage, NavigableSet<Long>> entry) {
                    QueryUuid.setCurrentThreadState(uuidState);

                    try {
                      List<Pair<Long, Long>> res = new ArrayList<>();

                      ColumnPage page = entry.getKey();
                      // take only those rowIDs that are inside the page - if there were row IDs provided that we do not
                      // have any values of, do not return those entries. This may happen for the last page of a column.
                      NavigableSet<Long> rowIds =
                          entry.getValue().headSet(page.getFirstRowId() + page.getValues().size(), false);

                      Long[] columnPageValueIds = new Long[rowIds.size()];
                      Iterator<Long> rowIdIt = rowIds.iterator();
                      if (columnPageValueIds.length >= page.getValues().size() / 3) {
                        // if we try to resolve more than 1/3 of the rowIds, decompress whole values array here and do
                        // not resolve 1-by-1.
                        // TODO #7 formalize and centralize this heurisitc
                        long[] allColPageValueIdsDecompressed = page.getValues().decompressedArray();
                        for (int i = 0; i < columnPageValueIds.length; i++)
                          columnPageValueIds[i] =
                              allColPageValueIdsDecompressed[(int) (rowIdIt.next() - page.getFirstRowId())];
                      } else {
                        for (int i = 0; i < columnPageValueIds.length; i++)
                          columnPageValueIds[i] = page.getValues().get((int) (rowIdIt.next() - page.getFirstRowId()));
                      }

                      Long[] columnValueIds = page.getColumnPageDict().decompressValues(columnPageValueIds);

                      rowIdIt = rowIds.iterator();
                      for (int i = 0; i < columnPageValueIds.length; i++)
                        res.add(new Pair<Long, Long>(rowIdIt.next(), columnValueIds[i]));

                      return res.stream();
                    } finally {
                      QueryUuid.clearCurrent();
                    }
                  }
                }).collect(() -> new HashMap<Long, Long>(), (map, pair) -> map.put(pair.getLeft(), pair.getRight()),
                    (map1, map2) -> map1.putAll(map2));
        return rowIdToColumnValueId;
      } finally {
        QueryUuid.setCurrentThreadState(uuidState);
      }
    }

    Map<Long, Long> res = new HashMap<>();
    rowIds.forEach(rowId -> res.put(rowId, 0L));
    return res;
  }

  @Override
  public Long[] resolveColumnValueIdsForRowsFlat(Long[] rowIds) {
    if (delegate instanceof StandardColumnShard) {
      Map<Long, Long> rowIdToColumnValueId = resolveColumnValueIdsForRows(Arrays.asList(rowIds));
      Long[] res = new Long[rowIds.length];
      for (int i = 0; i < rowIds.length; i++)
        res[i] = (rowIdToColumnValueId.containsKey(rowIds[i])) ? rowIdToColumnValueId.get(rowIds[i]) : -1L;

      return res;
    }
    Long[] res = new Long[rowIds.length];
    Arrays.fill(res, 0L);

    return res;
  }

  @Override
  public long resolveColumnValueIdForRow(Long rowId) {
    if (delegate instanceof StandardColumnShard) {
      Entry<Long, ColumnPage> floorEntry = ((StandardColumnShard) delegate).getPages().floorEntry(rowId);
      if (floorEntry == null)
        return -1;
      ColumnPage page = floorEntry.getValue();
      if (rowId >= page.getFirstRowId() + page.getValues().size())
        return -1;

      queryRegistry.getOrCreateCurrentStatsManager().registerPageAccess(page, isTempColumn);

      return page.getColumnPageDict().decompressValue(page.getValues().get((int) (rowId - page.getFirstRowId())));
    }

    return 0;
  }

  @Override
  public Set<Pair<Long, Integer>> getGoodResolutionPairs() {
    if (delegate instanceof StandardColumnShard) {
      Set<Pair<Long, Integer>> res = ((StandardColumnShard) delegate).getPages().entrySet().stream(). //
          map(entry -> new Pair<Long, Integer>(entry.getKey(), entry.getValue().size())). //
          collect(Collectors.toSet());

      return res;
    }

    return new HashSet<>(Arrays.asList(new Pair<>(delegate.getFirstRowId(), 1)));
  }

  @Override
  public ColumnShard getDelegate() {
    return delegate;
  }

}
