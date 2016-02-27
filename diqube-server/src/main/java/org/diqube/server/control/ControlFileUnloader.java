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
package org.diqube.server.control;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.diqube.data.table.AdjustableTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.TableRegistry;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unloads he data that was loaded from a specific control file by {@link ControlFileLoader}.
 *
 * @author Bastian Gloeckle
 */
public class ControlFileUnloader {
  private static final Logger logger = LoggerFactory.getLogger(ControlFileUnloader.class);

  private File controlFile;

  private TableRegistry tableRegistry;

  private Pair<String, List<Long>> tableInfo;

  private TableMetadataManager tableMetadataManager;

  /**
   * @param tableInfo
   *          Result of {@link ControlFileLoader} when the control file was loaded. Pair of table name and list of
   *          firstROwIds of the loaded shards.
   */
  /* package */ ControlFileUnloader(TableRegistry tableRegistry, TableMetadataManager tableMetadataManager,
      File controlFile, Pair<String, List<Long>> tableInfo) {
    this.tableRegistry = tableRegistry;
    this.tableMetadataManager = tableMetadataManager;
    this.controlFile = controlFile;
    this.tableInfo = tableInfo;
  }

  /**
   * Unloads the table from the local {@link TableRegistry} and takes care to update {@link TableMetadata} in the
   * cluster accordingly (if wanted).
   * 
   * Note that .ready files will not be removed.
   * 
   * @param handleMetadataChange
   *          If this method should adjust the metadata of the table in the cluster according to the changes.
   */
  public void unload(boolean handleMetadataChange) {
    Table t = tableRegistry.getTable(tableInfo.getLeft());
    if (t == null)
      logger.warn("Could not delete anything as table {} is not loaded (anymore?).", tableInfo.getLeft());
    else {
      logger.info(
          "Identified deletion of control file {}; will remove in-memory data from table {} for TableShards with starting rowIds {}.",
          controlFile.getAbsolutePath(), tableInfo.getLeft(), tableInfo.getRight());
      List<TableShard> shardsToDelete = t.getShards().stream()
          .filter(s -> tableInfo.getRight().contains(s.getLowestRowId())).collect(Collectors.toList());
      shardsToDelete.forEach(s -> ((AdjustableTable) t).removeTableShard(s));
      if (t.getShards().isEmpty()) {
        logger.info("Removed last table shard of table '{}', will stop serving this table completely.",
            tableInfo.getLeft());
        tableRegistry.removeTable(tableInfo.getLeft());
      }

      // we removed something, metadata might have changed (=can only be "reduced"), therefore ask the whole cluster
      // to recompute the metadata fully. Note that even if we removed the last shard locally, the cluster might still
      // contain data of this table!
      if (handleMetadataChange)
        tableMetadataManager.startRecomputingTableMetadata(tableInfo.getLeft());

      // give garbage collector a hint that it might be able to free up some memory...
      System.gc();
    }
  }
}
