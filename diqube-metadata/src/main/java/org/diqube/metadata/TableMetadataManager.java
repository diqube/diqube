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
package org.diqube.metadata;

import java.util.function.Function;

import org.diqube.metadata.create.TableMetadataRecomputeRequestListener;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.TableMetadata;

/**
 * Manages table metadata across the cluster using an internal consensus state machine.
 *
 * <p>
 * This class manages calling the {@link TableMetadataRecomputeRequestListener}s in a separate thread pool.
 *
 * @author Bastian Gloeckle
 */
public interface TableMetadataManager {

  /**
   * Get current metadata of a specific table.
   * 
   * @param tableName
   *          Name of the table to get the metadata of. Can either be a normal tablename, or the name of a flattened
   *          table. The latter can either be a full one (with the flatten ID = one created by
   *          {@link FlattenedTableNameUtil#createFlattenedTableName(String, String, java.util.UUID)}) or one without
   *          the ID (such as the ones used in diql queries; in this case, the newest locally available flattening will
   *          be used; such a name can be created using
   *          {@link FlattenedTableNameUtil#createIncompleteFlattenedTableName(String, String)}).
   * @return Current global {@link TableMetadata} for the table or <code>null</code> if no metadata is available.
   * @throws AuthorizationException
   *           In case an incomplete flatten table name was provided and there are no nodes serving the original table.
   */
  public TableMetadata getCurrentTableMetadata(String tableName) throws AuthorizationException;

  /**
   * Adjust the current metadata for a given table.
   * 
   * <p>
   * Note that this method might retry and the adjustFunction might be called multiple times therefore.
   * 
   * <p>
   * Although the adjustfunction in theory could change the TableMetadata arbitrarily, in a typical usecase only data
   * might be /added/ to the TableMetadata - since we usually will merge a new TableMetadat into the one that is
   * available - and merging will only add additional data, nt remove any. So, when data is actually removed (like e.g.
   * a TableShard is unloaded on the current node), we cannot "remove" the TableMetadata of that shard from the global
   * state, since we cannot compute that "reverse delta". For that usecase, the metadata of the whole table should be
   * recomputed fully, using {@link #startRecomputingTableMetadata(String)}.
   * 
   * @param adjustFunction
   *          Function called to adjust the current TableMetadata that is available in the cluster. If no table metadata
   *          is available, the parameter passed to this function is <code>null</code>. The function must return a valid
   *          new {@link TableMetadata} object that should be set for the table in the cluster.
   */
  public void adjustTableMetadata(String tableName, Function<TableMetadata, TableMetadata> adjustFunction);

  /**
   * Marks the currently available metadata as invalid and asks all cluster nodes to recompute their TableMetadata and
   * push them again to the new overall state.
   * 
   * This should be called after e.g. unloading a TableShard on the local machine, see description of
   * {@link #adjustTableMetadata(String, Function)}.
   */
  public void startRecomputingTableMetadata(String tableName);

}
