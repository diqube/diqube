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
package org.diqube.server.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.TableRegistry;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.metadata.create.TableMetadataMerger;
import org.diqube.metadata.create.TableShardMetadataBuilderFactory;
import org.diqube.metadata.create.TableMetadataMerger.IllegalTableLayoutException;
import org.diqube.metadata.create.TableShardMetadataBuilder.IllegalTableShardLayoutException;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to publish the metadata of a all local {@link TableShard}s of a single table to the metadata cluster.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ServerTableMetadataPublisher {
  private static final Logger logger = LoggerFactory.getLogger(ServerTableMetadataPublisher.class);

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private TableShardMetadataBuilderFactory tableShardMetadataBuilderFactory;

  @Inject
  private TableMetadataManager tableMetadataManager;

  /**
   * Calculates and publishes the (local) metadata of the given table to the cluster by calling
   * {@link TableMetadataManager#adjustTableMetadata(String, java.util.function.Function)} synchronously.
   * 
   * @throws TableNotExistsException
   *           if table does not exist locally.
   * @throws MergeImpossibleException
   *           If metadata cannot be merged.
   */
  public void publishMetadataOfTable(String tableName) throws TableNotExistsException, MergeImpossibleException {
    Table t = tableRegistry.getTable(tableName);
    if (t == null)
      throw new TableNotExistsException("Table '" + tableName + "' does not exist.");

    Collection<TableShard> shards = t.getShards();
    publishMetadataOfTableShards(tableName, shards);
  }

  /**
   * Calculates and publishes metadata of the shards of a specfic local table to the cluster.
   * 
   * @throws MergeImpossibleException
   *           If merge is impossible.
   */
  public void publishMetadataOfTableShards(String tableName, Collection<TableShard> shards)
      throws MergeImpossibleException {
    List<TableMetadata> shardMetadata = new ArrayList<>();
    TableMetadata localMerged;
    try {
      for (TableShard s : shards)
        shardMetadata.add(tableShardMetadataBuilderFactory.createTableShardMetadataBuilder().from(s).build());

      localMerged = new TableMetadataMerger().of(shardMetadata).merge();
    } catch (IllegalTableShardLayoutException | IllegalTableLayoutException e) {
      String msg = "While recomputing the table metadata of table '" + tableName + "' an error was encountered.";
      logger.error(msg, e);
      throw new MergeImpossibleException(msg, e);
    }

    MergeImpossibleException failure[] = new MergeImpossibleException[1];
    failure[0] = null;

    tableMetadataManager.adjustTableMetadata(tableName, oldMetadata -> {
      if (oldMetadata == null)
        return localMerged;

      try {
        return new TableMetadataMerger().of(localMerged, oldMetadata).merge();
      } catch (Exception e) {
        // could not merge our local results with the clusters'.
        String msg = "While recomputing the table metadata of table '" + tableName + "' it was found that the "
            + "metadata of this node is incompatible with the metadata of the rest of the cluster.";
        logger.error(msg, e);
        failure[0] = new MergeImpossibleException(msg, e);
        return oldMetadata;
      }
    });

    if (failure[0] != null)
      throw failure[0];
  }

  /* package */void setTableRegistry(TableRegistry tableRegistry) {
    this.tableRegistry = tableRegistry;
  }

  /* package */void setTableShardMetadataBuilderFactory(
      TableShardMetadataBuilderFactory tableShardMetadataBuilderFactory) {
    this.tableShardMetadataBuilderFactory = tableShardMetadataBuilderFactory;
  }

  /* package */void setTableMetadataManager(TableMetadataManager tableMetadataManager) {
    this.tableMetadataManager = tableMetadataManager;
  }

  public static class TableNotExistsException extends Exception {
    private static final long serialVersionUID = 1L;

    public TableNotExistsException(String msg) {
      super(msg);
    }
  }

  public static class MergeImpossibleException extends Exception {
    private static final long serialVersionUID = 1L;

    public MergeImpossibleException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
