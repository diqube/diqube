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
package org.diqube.execution;

import java.util.Deque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.flatten.Flattener;
import org.diqube.util.Pair;

/**
 * Manages various {@link FlattenedTable}s that are available locally.
 *
 * TODO #27 remove unneeded flattened versions. Make sure to not remove a version that is still needed.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenedTableManager {
  /**
   * Number of seconds a flattened table gets marked by {@link #getNewestFlattenedTableVersionAndMarkIt(String, String)}
   * and therefore saves it from being evicted.
   */
  public static final int FLATTENED_TABLE_MARK_SECONDS = 300;

  private Map<Pair<String, String>, Deque<FlattenedTableInfo>> tables = new ConcurrentHashMap<>();

  /**
   * Register a newly created {@link FlattenedTable} from {@link Flattener}.
   * 
   * <p>
   * This version will automatically be the newest version available, so it is likely that
   * {@link #getNewestFlattenedTableVersion(String, String)} will return this flattened table version if called right
   * after registering the new version.
   * 
   * @param versionId
   *          The version ID of the flattened table.
   * @param flattenedTable
   *          The flattened table itself.
   * @param origTableName
   *          The table the flattened table was based on.
   * @param flattenBy
   *          The field which the table was flattened by.
   */
  public void registerFlattenedTableVersion(UUID versionId, FlattenedTable flattenedTable, String origTableName,
      String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<FlattenedTableInfo> deque = tables.get(keyPair);

    FlattenedTableInfo newInfo = new FlattenedTableInfo(versionId, flattenedTable);

    if (deque == null) {
      deque = new ConcurrentLinkedDeque<>();
      deque.addFirst(newInfo);
      Deque<FlattenedTableInfo> finalDeque = deque;

      tables.merge(keyPair, deque, (oldDeque, newDeque) -> {
        oldDeque.addAll(finalDeque);
        return oldDeque;
      });
    } else
      deque.addFirst(newInfo);
  }

  /**
   * Fetches the newest version of a flattened table.
   * 
   * @param origTableName
   *          Name of the original table.
   * @param flattenBy
   *          Field by which the orig table was flattened.
   * @return Pair of version ID and flattened table, or <code>null</code> in case there is no flattened version of that
   *         table.
   */
  public Pair<UUID, FlattenedTable> getNewestFlattenedTableVersion(String origTableName, String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<FlattenedTableInfo> deque = tables.get(keyPair);
    if (deque == null)
      return null;

    try {
      FlattenedTableInfo info = deque.getFirst();
      return new Pair<>(info.getVersionId(), info.getFlattenedTable());
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  /**
   * Fetches the newest version of a flattened table and marks that version to not be evicted by
   * {@link #FLATTENED_TABLE_MARK_SECONDS} seconds.
   * 
   * @param origTableName
   *          Name of the original table.
   * @param flattenBy
   *          Field by which the orig table was flattened.
   * @return Pair of version ID and flattened table, or <code>null</code> in case there is no flattened version of that
   *         table.
   */
  public Pair<UUID, FlattenedTable> getNewestFlattenedTableVersionAndMarkIt(String origTableName, String flattenBy) {
    // TODO #27

    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<FlattenedTableInfo> deque = tables.get(keyPair);
    if (deque == null)
      return null;

    try {
      FlattenedTableInfo info = deque.getFirst();
      return new Pair<>(info.getVersionId(), info.getFlattenedTable());
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  /**
   * Get a specific version of a flattened table.
   * 
   * @param versionId
   *          The version of the flattened table.
   * @param origTableName
   *          The table name the flattening was based on.
   * @param flattenBy
   *          The field by which the original table was flattened.
   * @return The {@link FlattenedTable} or <code>null</code> if it is not available.
   */
  public FlattenedTable getFlattenedTable(UUID versionId, String origTableName, String flattenBy) {
    Pair<String, String> keyPair = new Pair<>(origTableName, flattenBy);
    Deque<FlattenedTableInfo> deque = tables.get(keyPair);
    if (deque == null)
      return null;

    for (FlattenedTableInfo info : deque) {
      if (info.getVersionId().equals(versionId))
        return info.getFlattenedTable();
    }
    return null;
  }

  private class FlattenedTableInfo {
    private UUID versionId;
    private FlattenedTable flattenedTable;

    public FlattenedTableInfo(UUID versionId, FlattenedTable flattenedTable) {
      this.versionId = versionId;
      this.flattenedTable = flattenedTable;
    }

    public UUID getVersionId() {
      return versionId;
    }

    public FlattenedTable getFlattenedTable() {
      return flattenedTable;
    }
  }
}
