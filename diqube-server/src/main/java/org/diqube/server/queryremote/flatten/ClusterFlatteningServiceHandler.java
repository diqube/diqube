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
package org.diqube.server.queryremote.flatten;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.connection.ConnectionOrLocalHelper;
import org.diqube.cluster.connection.ServiceProvider;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.execution.FlattenedTableManager;
import org.diqube.execution.TableRegistry;
import org.diqube.flatten.FlattenUtil;
import org.diqube.flatten.QueryMasterFlattenService;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.ClusterFlatteningServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterFlatteningService;
import org.diqube.remote.cluster.thrift.RFlattenException;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.remote.cluster.thrift.RRetryLaterException;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for {@link ClusterFlatteningService}, which handles flattening local tables.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterFlatteningServiceHandler implements ClusterFlatteningService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterFlatteningServiceHandler.class);

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private QueryMasterFlattenService queryMasterFlattenService;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private FlattenUtil flattenUtil;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private ClusterManager clusterManager;

  @Inject
  private FlattenedTableManager flattenedTableManager;

  @Config(ConfigKey.FLATTEN_TIMEOUT_SECONDS)
  private int flattenTimeoutSeconds;

  private Map<Long, UUID> requestIdByThreadId = new ConcurrentHashMap<>();

  private ExecutorService flatteningExecutor;

  private Map<UUID, Object> flattenRequestSync = new ConcurrentHashMap<>();

  private Map<UUID, Deque<Map<Long, Long>>> flattenRequestOtherFlattenResults = new ConcurrentHashMap<>();

  private Map<UUID, RNodeAddress> flattenRequestResultAddress = new ConcurrentHashMap<>();

  @PostConstruct
  public void initialize() {
    flatteningExecutor = executorManager.newCachedThreadPoolWithMax("flatten-%d", new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        UUID requestUuid = requestIdByThreadId.remove(t.getId());

        logger.warn("Uncaught exception while processing flatten request {}", requestUuid, e);

        if (requestUuid != null) {
          flattenRequestOtherFlattenResults.remove(requestUuid);
          flattenRequestSync.remove(requestUuid);
          RNodeAddress resultNode = flattenRequestResultAddress.remove(requestUuid);

          if (resultNode != null) {
            // try to send that our flattening failed.
            try (ServiceProvider<ClusterFlatteningService.Iface> serviceProv = connectionOrLocalHelper.getService(
                ClusterFlatteningService.Client.class, ClusterFlatteningService.Iface.class,
                ClusterFlatteningServiceConstants.SERVICE_NAME, resultNode, null)) {

              serviceProv.getService().flatteningFailed(RUuidUtil.toRUuid(requestUuid),
                  new RFlattenException(e.getMessage()));
            } catch (Exception e2) {
              logger.error("Could not send 'flattening failed' for flattening request {} to result node {}. Ignoring.",
                  requestUuid, resultNode, e2);
            }
          }
        }
      }
    }, 3);
  }

  @PreDestroy
  public void shutdown() {
    flatteningExecutor.shutdownNow();
  }

  @Override
  public ROptionalUuid getLatestValidFlattening(String tableName, String flattenBy)
      throws RFlattenException, TException {
    Table table = tableRegistry.getTable(tableName);
    if (table == null)
      throw new RFlattenException("Table '" + tableName + "' unknown.");

    Pair<UUID, FlattenedTable> newest = flattenedTableManager.getNewestFlattenedTableVersion(tableName, flattenBy);
    if (newest == null)
      return new ROptionalUuid();

    // check if the FlattenedTable is "valid", i.e. it has flattened exactly those shards that are available in the
    // table! As the table can be extended/shrinked over time (add/remove .control files), it could happen that there is
    // something flattened that is actually not valid any more.

    FlattenedTable flattenedTable = newest.getRight();
    Set<Long> firstRowIds = table.getShards().stream().map(shard -> shard.getLowestRowId()).collect(Collectors.toSet());

    // we identify validity if the flattenedTable was based on those "firstRowId" values of TableShards that the current
    // table contains: One shard (identified by its firstRowId) will never change its content. So either shards could
    // have been added or removed, but if the firstRowIds are equal, wthe table should be equal.
    if (!flattenedTable.getOriginalFirstRowIdsOfShards().equals(firstRowIds))
      return new ROptionalUuid();

    ROptionalUuid res = new ROptionalUuid();
    res.setUuid(RUuidUtil.toRUuid(newest.getLeft()));
    return res;
  }

  @Override
  public void flattenAllLocalShards(RUUID flattenRequestId, String tableName, String flattenBy,
      List<RNodeAddress> otherFlatteners, RNodeAddress resultAddress) throws RFlattenException, TException {
    Table table = tableRegistry.getTable(tableName);
    if (table == null)
      throw new RFlattenException("Table '" + tableName + "' not available.");

    UUID requestUuid = RUuidUtil.toUuid(flattenRequestId);

    logger.info("Starting to flatten '{}' by '{}', request ID {}, result addr {}, other flatteners: {}", tableName,
        flattenBy, requestUuid, resultAddress, otherFlatteners);

    flattenRequestSync.put(requestUuid, new Object());
    flattenRequestOtherFlattenResults.put(requestUuid, new ConcurrentLinkedDeque<>());
    flattenRequestResultAddress.put(requestUuid, resultAddress);

    // execute asynchronously.
    flatteningExecutor.execute(() -> {
      requestIdByThreadId.put(Thread.currentThread().getId(), requestUuid);

      // fetch table shards in one go - otherwise they might change inside the table while we're processing them!
      List<TableShard> inputShards = table.getShards().stream()
          .sorted((s1, s2) -> Long.compare(s1.getLowestRowId(), s2.getLowestRowId())).collect(Collectors.toList());

      FlattenedTable flattenedTable = flattenUtil.flattenTable(table, inputShards, flattenBy);

      List<TableShard> flattenedShards = flattenedTable.getShards().stream()
          .sorted((s1, s2) -> Long.compare(s1.getLowestRowId(), s2.getLowestRowId())).collect(Collectors.toList());

      Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRowsDelta = new HashMap<>();
      NavigableMap<Long, TableShard> origShardFirstRowIdToFlattenedShard = new TreeMap<>();
      for (int i = 0; i < inputShards.size(); i++) {
        origShardFirstRowIdToFlattenedNumberOfRowsDelta.put(inputShards.get(i).getLowestRowId(),
            flattenedShards.get(i).getNumberOfRowsInShard() - inputShards.get(i).getNumberOfRowsInShard());
        origShardFirstRowIdToFlattenedShard.put(inputShards.get(i).getLowestRowId(), flattenedShards.get(i));
      }

      logger.trace(
          "Flattening '{}' by '{}', request ID {}, completed local computation, sending results to other flatteners.",
          tableName, flattenBy, requestUuid);

      for (RNodeAddress otherFlattener : otherFlatteners) {
        boolean retry = false;
        int retryCountLeft = 10;
        while (retry) {
          retry = false;
          try (ServiceProvider<ClusterFlatteningService.Iface> serviceProv = connectionOrLocalHelper.getService(
              ClusterFlatteningService.Client.class, ClusterFlatteningService.Iface.class,
              ClusterFlatteningServiceConstants.SERVICE_NAME, otherFlattener, null)) {

            serviceProv.getService().shardsFlattened(flattenRequestId, origShardFirstRowIdToFlattenedNumberOfRowsDelta,
                clusterManager.getOurHostAddr().createRemote());
          } catch (RRetryLaterException e) {
            if (retryCountLeft == 0)
              // let the uncaughtExceptionHandler handle this...
              throw new RuntimeException("Exception while communicating with other flatteners: " + e.getMessage(), e);

            // we retry sending our results since the other flatteners might not have initialized for this request yet.
            try {
              Thread.sleep(1000); // 1s
            } catch (InterruptedException e1) {
              // let uncaughtExceptionHandler clean up.
              throw new RuntimeException("Interrupted while waiting for other flatteners to get ready.", e1);
            }
            retryCountLeft--;
            retry = true;
          } catch (Exception e) {
            // let the uncaughtExceptionHandler handle this...
            throw new RuntimeException("Exception while communicating with other flatteners: " + e.getMessage(), e);
          }
        }
      }

      // We now flattened our shard. Before we can use it, though, we need to wait for the results of all other
      // flatteners, as we need to adjust our rowIds accordingly.

      int numberOfOtherFlattenersResponded = 0;
      long timeoutTime = System.nanoTime() + flattenTimeoutSeconds * 1_000_000_000;

      logger.trace("Flattening '{}' by '{}', request ID {}, waiting for results from other flatteners", tableName,
          flattenBy, requestUuid);

      while (numberOfOtherFlattenersResponded < otherFlatteners.size()) {
        synchronized (flattenRequestSync.get(requestUuid)) {
          if (flattenRequestOtherFlattenResults.get(requestUuid).isEmpty())
            try {
              flattenRequestSync.get(requestUuid).wait(1000); // 1s
            } catch (InterruptedException e) {
              // let the uncaughtExceptionHandler handle this...
              throw new RuntimeException("Interrupted while waiting for results of other nodes: " + e.getMessage(), e);
            }
        }

        if (System.nanoTime() > timeoutTime)
          throw new RuntimeException("Timed out waiting for other flatteners to calculate their result.");

        Map<Long, Long> otherFlattenerResult;
        while ((otherFlattenerResult = flattenRequestOtherFlattenResults.get(requestUuid).poll()) != null) {
          numberOfOtherFlattenersResponded++;
          for (Entry<Long, Long> otherEntry : otherFlattenerResult.entrySet()) {
            long otherOrigFirstRowId = otherEntry.getKey();
            long otherFlattenedNumerOfRowsDelta = otherEntry.getValue();

            if (otherOrigFirstRowId < flattenedShards.get(flattenedShards.size() - 1).getLowestRowId()) {
              // another shard which is based on a shard before ours was flattened: We need to adjust our rowIds!

              Map<Long, TableShard> affectedShards =
                  origShardFirstRowIdToFlattenedShard.headMap(otherOrigFirstRowId, false);
              for (TableShard tableShard : affectedShards.values()) {
                for (StandardColumnShard colShard : tableShard.getColumns().values()) {
                  ((AdjustableStandardColumnShard) colShard)
                      .adjustToFirstRowId(colShard.getFirstRowId() + otherFlattenedNumerOfRowsDelta);
                }
              }
            }
          }
        }
      }

      // Okay, all results from other flatteners received, we're done!
      UUID flattenedTableId = requestUuid;
      flattenedTableManager.registerFlattenedTableVersion(flattenedTableId, flattenedTable, tableName, flattenBy);

      // not in "finally", since we do not want to clear this here if we have an exception -> the
      // uncaughtExceptionHandler will handle that case!
      requestIdByThreadId.remove(Thread.currentThread().getId());
      flattenRequestOtherFlattenResults.remove(requestUuid);
      flattenRequestSync.remove(requestUuid);
      flattenRequestResultAddress.remove(requestUuid);

      logger.info("Finished flattening '{}' by '{}', request ID {}.", tableName, flattenBy, requestUuid);

      try (ServiceProvider<ClusterFlatteningService.Iface> serviceProv = connectionOrLocalHelper.getService(
          ClusterFlatteningService.Client.class, ClusterFlatteningService.Iface.class,
          ClusterFlatteningServiceConstants.SERVICE_NAME, resultAddress, null)) {

        serviceProv.getService().flatteningDone(flattenRequestId, RUuidUtil.toRUuid(flattenedTableId),
            clusterManager.getOurHostAddr().createRemote());
      } catch (Exception e) {
        logger.warn("Could not send flattening result {} to requesting machine at {}. Ignoring.", requestUuid,
            resultAddress, e);
      }
    });
  }

  @Override
  public void shardsFlattened(RUUID flattenRequestId, Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRowsDelta,
      RNodeAddress flattener) throws TException, RRetryLaterException {
    UUID requestId = RUuidUtil.toUuid(flattenRequestId);
    Object sync = flattenRequestSync.get(requestId);
    if (sync == null)
      throw new RRetryLaterException("Local sync object not ready.");

    synchronized (sync) {
      Deque<Map<Long, Long>> deque = flattenRequestOtherFlattenResults.get(requestId);
      if (deque == null)
        throw new RRetryLaterException("Local queue not ready.");
      deque.add(origShardFirstRowIdToFlattenedNumberOfRowsDelta);

      sync.notifyAll();
    }
  }

  @Override
  public void flatteningDone(RUUID flattenRequestId, RUUID flattenedTableId, RNodeAddress flattener) throws TException {
    // executed on query master node.
    queryMasterFlattenService.singleRemoteCompletedFlattening(RUuidUtil.toUuid(flattenRequestId),
        RUuidUtil.toUuid(flattenedTableId), flattener);
  }

  @Override
  public void flatteningFailed(RUUID flattenRequestId, RFlattenException flattenException) throws TException {
    // executed on query master node.
    queryMasterFlattenService.singleRemoteFailedFlattening(RUuidUtil.toUuid(flattenRequestId),
        flattenException.getMessage());
  }

}