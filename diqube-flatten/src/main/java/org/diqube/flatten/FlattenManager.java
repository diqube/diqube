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

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.FlattenedTableInstanceManager;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages providing "freshly flattened" tables.
 * 
 * <p>
 * "Freshly flattened" tables are instances of {@link FlattenedTable} whose firstRowIds of its table shards align with
 * the ones returned by {@link FlattenedTable#getOriginalFirstRowIdsOfShards()} and whose firstRowIds of the table
 * shards can be adjusted freely without interfering with any other object instance of {@link FlattenedTable}. Note that
 * the returned {@link FlattenedTable} will not be usable right away, as row IDs will overlap - it needs to be adjusted.
 * This is described in
 * {@link Flattener#flattenTable(org.diqube.data.table.Table, java.util.Collection, String, java.util.UUID)}, too.
 * 
 * <p>
 * The flattened tables that are provided by this class might originate from different sources:
 * 
 * <ul>
 * <li>The table might be already flattened, valid (see {@link FlattenedTable#getOriginalFirstRowIdsOfShards()}) and
 * available through {@link FlattenedTableInstanceManager} in which case
 * {@link FlattenedTableUtil#facadeWithDefaultRowIds(FlattenedTable, String, String, java.util.UUID)} will be used to
 * return a non-interfering instance of that {@link FlattenedTable}.
 * <li>A valid version of the flattened table might have been flattened before and be available on disk in the
 * {@link FlattenedTableDiskCache}. In this case that version will be loaded from there.
 * <li>If none of the above, the table will be flattened newly with the {@link Flattener}.
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenManager {
  private static final Logger logger = LoggerFactory.getLogger(FlattenManager.class);

  @Inject
  private FlattenedTableInstanceManager flattenedTableInstanceManager;

  @Inject
  private FlattenedTableUtil flattenedTableUtil;

  @Inject
  private FlattenedTableDiskCache flattenedTableDiskCache;

  @Inject
  private Flattener flattener;

  /**
   * Creates a new instance of a {@link FlattenedTable} for the given source table/flatten by.
   * 
   * <p>
   * On the returned table it is safe to call {@link AdjustableStandardColumnShard#adjustToFirstRowId(long)}, as that
   * change will not interfere with any other instances of that flattened table, although this method tries to re-use
   * the flattened data as well as possible.
   * 
   * <p>
   * The source data of the returned {@link FlattenedTable} might either come from the
   * {@link FlattenedTableInstanceManager}, from an {@link FlattenedTableDiskCache} or a new flattening will be created
   * using {@link Flattener}.
   * 
   * <p>
   * Note that the returned {@link FlattenedTable} is not yet ready to be used, as most likely the rowIds of the
   * TableShards will overlap, since the returned {@link FlattenedTable}s shards will have the same firstRowIds as the
   * original table has, but typically a each TableShard in a flattened table will contain more rows than its
   * counterpart in the not-flattened one. See also result of
   * {@link Flattener#flattenTable(Table, Collection, String, UUID)}.
   * 
   * @param sourceTable
   *          The table to get a flattened version of
   * @param sourceTableShards
   *          the table shards to be flattened. If null, the tableShards of the sourceTable will be used.
   * @param flattenBy
   *          Which field to flatten by.
   * @param flattenId
   *          The ID of the resulting flattening.
   * @return A fresh instance of {@link FlattenedTable}.
   */
  public FlattenedTable createFlattenedTable(Table sourceTable, Collection<TableShard> sourceTableShards,
      String flattenBy, UUID flattenId) {
    if (sourceTableShards == null)
      sourceTableShards = sourceTable.getShards();

    Set<Long> sourceOriginalFirstRowIds =
        sourceTableShards.stream().map(shard -> shard.getLowestRowId()).collect(Collectors.toSet());

    // check instance manager if a valid version is loaded in memory already.
    Pair<UUID, FlattenedTable> newestInstancePair =
        flattenedTableInstanceManager.getNewestFlattenedTableVersion(sourceTable.getName(), flattenBy);
    if (newestInstancePair != null) {
      FlattenedTable newestInstance = newestInstancePair.getRight();
      if (sourceOriginalFirstRowIds.equals(newestInstance.getOriginalFirstRowIdsOfShards())) {
        // "newest" is still valid. Great!
        logger.info("Will re-use the flattening for '{}' by '{}' from ID {} for new ID {}", sourceTable.getName(),
            flattenBy, newestInstancePair.getLeft(), flattenId);
        return flattenedTableUtil.facadeWithDefaultRowIds(newestInstance, sourceTable.getName(), flattenBy, flattenId);
      }
    }

    // check disk cache.
    FlattenedTable diskCacheInstance =
        flattenedTableDiskCache.load(sourceTable.getName(), flattenBy, sourceOriginalFirstRowIds);
    if (diskCacheInstance != null) {
      // disk cache has a version, wohoo!
      logger.info("Will re-use the disk-cached version for '{}' by '{}' for new ID {}", sourceTable.getName(),
          flattenBy, flattenId);
      return flattenedTableUtil.facadeWithDefaultRowIds(diskCacheInstance, sourceTable.getName(), flattenBy, flattenId);
    }

    // Create new flatten.
    logger.info("No valid flattened table for '{}' by '{}' available, will therefore flatten table now.",
        sourceTable.getName(), flattenBy);
    FlattenedTable res = flattener.flattenTable(sourceTable, sourceTableShards, flattenBy, flattenId);
    flattenedTableDiskCache.offer(res, sourceTable.getName(), flattenBy);
    return res;
  }
}
