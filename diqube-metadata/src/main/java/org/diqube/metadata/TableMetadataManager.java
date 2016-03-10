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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.consensus.internal.DiqubeCatalystSerializer;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.metadata.consensus.TableMetadataStateMachine;
import org.diqube.metadata.consensus.TableMetadataStateMachine.CompareAndSetTableMetadata;
import org.diqube.metadata.consensus.TableMetadataStateMachine.GetTableMetadata;
import org.diqube.metadata.consensus.TableMetadataStateMachine.RecomputeTableMetadata;
import org.diqube.metadata.consensus.TableMetadataStateMachineImplementation;
import org.diqube.metadata.create.TableMetadataRecomputeRequestListener;
import org.diqube.metadata.util.CurrentFlattenedTableNameUtil;
import org.diqube.metadata.util.CurrentFlattenedTableNameUtil.FlattenIdentificationImpossibleException;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.threads.ExecutorManager;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages table metadata across the cluster using an internal consensus state machine.
 *
 * <p>
 * This class manages calling the {@link TableMetadataRecomputeRequestListener}s in a separate thread pool.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableMetadataManager {
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataManager.class);

  @Inject
  private ConsensusClient consensusClient;

  @Inject
  private TableMetadataStateMachineImplementation tableMetadataStateMachineImplementation;

  @InjectOptional
  private List<TableMetadataRecomputeRequestListener> recomputeListeners;

  @InjectOptional
  private ExecutorManager executorManager;

  @Inject
  private DiqubeCatalystSerializer diqubeCatalystSerializer;

  @Inject
  private CurrentFlattenedTableNameUtil currentFlattenedTableNameUtil;

  @Inject
  private FlattenedTableNameUtil flattenedTableNameUtil;

  private ExecutorService recomputeExecutor;

  @PostConstruct
  public void initialize() {
    tableMetadataStateMachineImplementation.setRecomputeConsumer(tableName -> requestLocalRecomputation(tableName));

    recomputeExecutor =
        executorManager.newCachedThreadPoolWithMax("table-metadata-recompute-%d", new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception while recomputing table metadata. This node should be restarted.", e);
          }
        }, 1);
  }

  @PreDestroy
  public void cleanup() {
    if (recomputeExecutor != null)
      recomputeExecutor.shutdownNow();
  }

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
  public TableMetadata getCurrentTableMetadata(String tableName) throws AuthorizationException {
    if (flattenedTableNameUtil.isFlattenedTableName(tableName)
        && !flattenedTableNameUtil.isFullFlattenedTableName(tableName)) {
      try {
        tableName = currentFlattenedTableNameUtil.enhanceIncompleteFlattenedTableNameWithNewestFlattenId(tableName);
      } catch (FlattenIdentificationImpossibleException e) {
        logger.warn("Cannot get newest flatten ID for table '{}' of which metadata should've been loaded", tableName,
            e);
        // cannot identify any metadata.
        return null;
      }
    }

    Pair<TableMetadata, Long> current = null;
    try (ClosableProvider<TableMetadataStateMachine> p =
        consensusClient.getStateMachineClient(TableMetadataStateMachine.class)) {
      current = p.getClient().getTableMetadata(GetTableMetadata.local(tableName));
    } catch (ConsensusClusterUnavailableException e) {
      return null;
    }

    return (current != null) ? current.getLeft() : null;
  }

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
  public void adjustTableMetadata(String tableName, Function<TableMetadata, TableMetadata> adjustFunction) {
    try (ClosableProvider<TableMetadataStateMachine> p =
        consensusClient.getStateMachineClient(TableMetadataStateMachine.class)) {

      boolean success = false;
      while (!success) {
        Pair<TableMetadata, Long> currentMetadataPair =
            p.getClient().getTableMetadata(GetTableMetadata.local(tableName));

        TableMetadata newMetadata;
        long previousVersion;
        if (currentMetadataPair == null) {
          newMetadata = adjustFunction.apply(null);
          previousVersion = Long.MIN_VALUE;
        } else {
          newMetadata = adjustFunction.apply(currentMetadataPair.getLeft());
          previousVersion = currentMetadataPair.getRight();
        }

        try {
          diqubeCatalystSerializer.validateSerializationObject(newMetadata);
        } catch (IllegalArgumentException e) {
          logger.warn("Cannot publish metadata for table {} because metadata cannot be accepted by consensus cluster.",
              tableName, e);
          return;
        }

        success =
            p.getClient().compareAndSetTableMetadata(CompareAndSetTableMetadata.local(previousVersion, newMetadata));
      }

      logger.debug("Sent the current metadata of the local '{}' table to the cluster.", tableName);
    } catch (ConsensusClusterUnavailableException e) {
      logger.warn("Could not publish metadata becuase consensus cluster is not available", e);
    }
  }

  /**
   * Marks the currently available metadata as invalid and asks all cluster nodes to recompute their TableMetadata and
   * push them again to the new overall state.
   * 
   * This should be called after e.g. unloading a TableShard on the local machine, see description of
   * {@link #adjustTableMetadata(String, Function)}.
   */
  public void startRecomputingTableMetadata(String tableName) {
    try (ClosableProvider<TableMetadataStateMachine> p =
        consensusClient.getStateMachineClient(TableMetadataStateMachine.class)) {
      p.getClient().recomputeTableMetadata(RecomputeTableMetadata.local(tableName));
      logger.debug("Informing cluster that metadata for table '{}' needs to be recomputed.", tableName);
    } catch (ConsensusClusterUnavailableException e) {
      throw new IllegalStateException("Consensus cluster not available", e);
    }
  }

  /**
   * A request to recompute metadata has arrived us from another cluster node (perhaps even we ourselves started that!).
   */
  private void requestLocalRecomputation(String tableName) {
    if (recomputeListeners != null) {
      // execute listeners on different thread, because this method is effectively called by the state machine thread of
      // the consensus implementation - we must not block it. In addition, we expect the listeners to recompute and send
      // the results again -> another interaction with consensus, and this cannot happen while still working on applying
      // a commit to the local state machine.
      for (TableMetadataRecomputeRequestListener listener : recomputeListeners) {
        recomputeExecutor.execute(() -> listener.tableMetadataRecomputeRequestReceived(tableName));
      }
    }
  }

}
