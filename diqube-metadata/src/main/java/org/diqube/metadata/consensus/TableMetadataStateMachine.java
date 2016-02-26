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
package org.diqube.metadata.consensus;

import org.diqube.consensus.ConsensusMethod;
import org.diqube.consensus.ConsensusStateMachine;
import org.diqube.consensus.ConsensusUtil;
import org.diqube.data.metadata.TableMetadata;
import org.diqube.util.Pair;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.server.Commit;

/**
 * Consensus state machine that manages {@link TableMetadata}.
 *
 * TODO support removing table metadata and use it in a meaningful way - remove metadata only if table is removed across
 * whole cluster.
 *
 * @author Bastian Gloeckle
 */
@ConsensusStateMachine
public interface TableMetadataStateMachine {
  /**
   * Compare a specific version of metadata is still valid and then set the metadata to a new value.
   * 
   * @return true on success, false otherwise.
   */
  @ConsensusMethod(dataClass = CompareAndSetTableMetadata.class)
  public boolean compareAndSetTableMetadata(Commit<CompareAndSetTableMetadata> commit);

  /**
   * Return the currently valid TableMetadata and its version number.
   * 
   * @return <code>null</code> if not available. Note that also {@link Pair#getLeft()} can be null, but have a version
   *         number (in case the metadata is up for recomputation currently.
   */
  @ConsensusMethod(dataClass = GetTableMetadata.class,
      additionalSerializationClasses = { Pair.class, TableMetadata.class })
  public Pair<TableMetadata, Long> getTableMetadata(Commit<GetTableMetadata> commit);

  /**
   * Enforces all nodes who participate in the consensus cluster to recompute their TableMetadata and call
   * {@link #compareAndSetTableMetadata(Commit)} accordingly. This will only accept table names where there is metadata
   * available of currently.
   */
  @ConsensusMethod(dataClass = RecomputeTableMetadata.class)
  public void recomputeTableMetadata(Commit<RecomputeTableMetadata> commit);

  public static class CompareAndSetTableMetadata implements Command<Boolean> {
    private static final long serialVersionUID = 1L;

    private long previousMetadataVersion;

    private TableMetadata newMetadata;

    public long getPreviousMetadataVersion() {
      return previousMetadataVersion;
    }

    public TableMetadata getNewMetadata() {
      return newMetadata;
    }

    /**
     * @param previousMetadataVersion
     *          Use {@link Long#MIN_VALUE} if expected that there is no entry at all.
     */
    public static Commit<CompareAndSetTableMetadata> local(long previousMetadataVersion, TableMetadata newMetadata) {
      CompareAndSetTableMetadata res = new CompareAndSetTableMetadata();
      res.previousMetadataVersion = previousMetadataVersion;
      res.newMetadata = newMetadata;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class GetTableMetadata implements Query<Pair<TableMetadata, Long>> {
    private static final long serialVersionUID = 1L;

    private String table;

    public String getTable() {
      return table;
    }

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }

    public static Commit<GetTableMetadata> local(String table) {
      GetTableMetadata res = new GetTableMetadata();
      res.table = table;
      return ConsensusUtil.localCommit(res);
    }
  }

  public static class RecomputeTableMetadata implements Command<Void> {
    private static final long serialVersionUID = 1L;

    private String tableName;

    public String getTableName() {
      return tableName;
    }

    public static Commit<RecomputeTableMetadata> local(String tableName) {
      RecomputeTableMetadata res = new RecomputeTableMetadata();
      res.tableName = tableName;
      return ConsensusUtil.localCommit(res);
    }
  }
}
