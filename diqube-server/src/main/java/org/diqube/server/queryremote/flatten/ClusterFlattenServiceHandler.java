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
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.ServiceProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.FlattenedTableInstanceManager;
import org.diqube.executionenv.TableRegistry;
import org.diqube.flatten.FlattenManager;
import org.diqube.flatten.Flattener;
import org.diqube.flatten.QueryMasterFlattenService;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.RFlattenException;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.remote.cluster.thrift.RRetryLaterException;
import org.diqube.server.metadata.ServerTableMetadataPublisher;
import org.diqube.server.metadata.ServerTableMetadataPublisher.MergeImpossibleException;
import org.diqube.threads.ExecutorManager;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Handler for {@link ClusterFlatteningService}, which handles flattening local tables.
 *
 * <p>
 * See JavaDoc on {@link FlattenRunnable}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterFlattenServiceHandler implements ClusterFlattenService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterFlattenServiceHandler.class);

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private QueryMasterFlattenService queryMasterFlattenService;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Inject
  private ClusterManager clusterManager;

  @Inject
  private FlattenedTableInstanceManager flattenedTableInstanceManager;

  @Inject
  private FlattenManager flattenManager;

  @Inject
  private ServerTableMetadataPublisher metadataPublisher;

  @Config(ConfigKey.FLATTEN_TIMEOUT_SECONDS)
  private int flattenTimeoutSeconds;

  private ExecutorService flatteningExecutor;

  private Map<UUID, FlattenRequestDetails> requestDetails = new ConcurrentHashMap<>();

  private Map<Long, UUID> requestIdByThreadId = new ConcurrentHashMap<>();

  private Map<Pair<String, String>, UUID> currentFlattenRequest = new ConcurrentHashMap<>();

  @PostConstruct
  public void initialize() {
    flatteningExecutor = executorManager.newCachedThreadPoolWithMax("flatten-%d", new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        UUID requestUuid = requestIdByThreadId.remove(t.getId());

        logger.warn("Uncaught exception while processing flatten request {}", requestUuid, e);

        if (requestUuid != null) {
          FlattenRequestDetails details = requestDetails.remove(requestUuid);

          if (details != null) {
            synchronized (details.sync) {
              currentFlattenRequest.remove(details.requestPair);
            }

            // try to send that our flattening failed.
            for (Pair<RNodeAddress, UUID> resultPair : details.resultAddresses) {
              try (ServiceProvider<ClusterFlattenService.Iface> serviceProv =
                  connectionOrLocalHelper.getService(ClusterFlattenService.Iface.class, resultPair.getLeft(), null)) {

                serviceProv.getService().flattenFailed(RUuidUtil.toRUuid(resultPair.getRight()),
                    new RFlattenException(e.getMessage()));
              } catch (Exception e2) {
                logger.error(
                    "Could not send 'flattening failed' for flattening request {} to result node {}. Ignoring.",
                    resultPair.getRight(), resultPair.getLeft(), e2);
              }
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

    // flag it to not be removed from FlattenTableManager for some time (should be enough until our caller has received
    // answers from all remotes and can issue his query).
    Pair<UUID, FlattenedTable> newest =
        flattenedTableInstanceManager.getNewestFlattenedTableVersionAndFlagIt(tableName, flattenBy);
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

  /**
   * Flattens a table locally asynchronously and informs the {@link ClusterFlattenService} at the result address about
   * the process.
   * 
   * <p>
   * Note that the query master calls this method. If the execution fails, the query master should retry the process, as
   * explained on {@link FlattenRunnable}.
   * 
   * <p>
   * Note that requests might be merged and therefore different cluster nodes might actually return different
   * flattenedIds in the end - the master should take care of that by retrying the flatten process.
   * 
   * @param flattenRequestId
   *          A unique ID.
   * @param tableName
   *          Name of th etable to be flattened
   * @param flattenBy
   *          The field by which should be flattened. See {@link Flattener}.
   * @param otherFlatteners
   *          The other nodes that contain part of the source table and with which this node should communicate in order
   *          to clean up and rowId overlaps of the flattened table.
   * @param resultAddress
   *          Address where there's a {@link ClusterFlattenService} that will receive the results. Can be
   *          <code>null</code> to not send any results (only valid locally, since Thrift does not support null values).
   * @throws RFlattenException
   * @throws TException
   */
  @Override
  public void flattenAllLocalShards(RUUID flattenRequestId, String tableName, String flattenBy,
      List<RNodeAddress> otherFlatteners, RNodeAddress resultAddress) throws RFlattenException, TException {
    Table table = tableRegistry.getTable(tableName);
    if (table == null)
      throw new RFlattenException("Table '" + tableName + "' not available.");

    UUID requestUuid = RUuidUtil.toUuid(flattenRequestId);

    FlattenRequestDetails details = new FlattenRequestDetails();
    details.sync = new Object();
    details.otherFlattenResults = new ConcurrentLinkedDeque<>();
    details.resultAddresses = new ConcurrentLinkedDeque<>();
    if (resultAddress != null)
      details.resultAddresses.push(new Pair<>(resultAddress, requestUuid));
    details.requestPair = new Pair<>(tableName, flattenBy);

    requestDetails.put(requestUuid, details);

    // try to identify another request that is currently running and processing the same table.
    Pair<String, String> flattenRequestPair = new Pair<>(tableName, flattenBy);
    UUID otherRequestUuid = currentFlattenRequest.putIfAbsent(flattenRequestPair, requestUuid);
    if (otherRequestUuid != null && !requestUuid.equals(otherRequestUuid)) {
      FlattenRequestDetails otherDetails = requestDetails.get(otherRequestUuid);
      if (otherDetails != null) {
        synchronized (otherDetails.sync) {
          if (otherRequestUuid.equals(currentFlattenRequest.get(flattenRequestPair))) {
            logger.info(
                "Requested to flatten '{}' by '{}' in request id {}, but there is another flatten being "
                    + "executed for the same combination currently in request ID {}. Telling the latter request to"
                    + " inform the first request as soon as it is done.",
                tableName, flattenBy, requestUuid, otherRequestUuid);

            if (resultAddress != null)
              otherDetails.resultAddresses.push(new Pair<>(resultAddress, requestUuid));
            requestDetails.remove(requestUuid); // clean up our stuff.
            return;
          }
        }
      }
    }

    logger.info("Starting to flatten '{}' by '{}', request ID {}, result addr {}, other flatteners: {}", tableName,
        flattenBy, requestUuid, resultAddress, otherFlatteners);

    // execute asynchronously.
    flatteningExecutor.execute(new FlattenRunnable(requestUuid, details, table, tableName, flattenBy, otherFlatteners));
  }

  @Override
  public void shardsFlattened(RUUID flattenRequestId, Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRowsDelta,
      RNodeAddress flattener) throws TException, RRetryLaterException {
    UUID requestId = RUuidUtil.toUuid(flattenRequestId);
    logger.trace("Received a shardsFlattened result for flatten request {} from {}.", requestId, flattener);
    FlattenRequestDetails details = requestDetails.get(requestId);
    if (details == null)
      throw new RRetryLaterException("Local data not ready.");

    synchronized (details.sync) {
      details.otherFlattenResults.add(origShardFirstRowIdToFlattenedNumberOfRowsDelta);

      details.sync.notifyAll();
    }
  }

  @Override
  public void flattenDone(RUUID flattenRequestId, RUUID flattenedTableId, RNodeAddress flattener) throws TException {
    // executed on query master node.
    queryMasterFlattenService.singleRemoteCompletedFlattening(RUuidUtil.toUuid(flattenRequestId),
        RUuidUtil.toUuid(flattenedTableId), flattener);
  }

  @Override
  public void flattenFailed(RUUID flattenRequestId, RFlattenException flattenException) throws TException {
    // executed on query master node.
    queryMasterFlattenService.singleRemoteFailedFlattening(RUuidUtil.toUuid(flattenRequestId),
        flattenException.getMessage());
  }

  /**
   * Helper class which holds information of one flatten request that is currently being executed.
   */
  private static class FlattenRequestDetails {
    /** sync object used to sync access and inform the {@link FlattenRunnable} about new data received from remotes */
    Object sync;

    /** Addresses and their respective requestId which requested to execute this flatten process */
    Deque<Pair<RNodeAddress, UUID>> resultAddresses;

    /**
     * Results from other flatteners currently processing the same request. Maps from
     * "firstRowId of the unflattened tableShard which was flattened" to
     * "delta of rows the flattened shard has compared to the original".
     */
    Deque<Map<Long, Long>> otherFlattenResults;

    /** Pair of "tableName" and "flatten-by" */
    Pair<String, String> requestPair;
  }

  /**
   * Runnable that executes the flattening on this node, informs all other cluster nodes of our results and incorporates
   * the changes of those other cluster nodes into our result.
   * 
   * <p>
   * This process is a little bit more complex than it might seem at first. The Query Master triggers the flattening of
   * the requested table on all query remotes simultaneously. This runnable is executed on each of those. To achieve a
   * valid flattened version of the table, these query remotes have to talk to each other: When flattening, a table
   * typically has more rows than before, whcih means each TableShard of that table has more rows. Each row though must
   * have a unique rowId and to get these, the query remotes need to exchange information to not have overlapping
   * rowIds.
   * 
   * <p>
   * In addition to that, The {@link ClusterFlattenServiceHandler} merges multiple simultaneous requests to flatten the
   * same table by the same flatten-by-field: When one request is being processed and a second, equal, request is
   * received, that second request should be answered together with the first one - we do not want to calculate the very
   * same flattening twice. <br/>
   * This though contains even another small but important thing: If two query masters decide to flatten something at
   * the same time, part of the query remotes would receive the request from the first master first and the other part
   * of the query remotes would receive the request from the second master first. Therefore the query remotes would not
   * work on the same "flatten request ID". In that case, we simply fail the flattening process. This happens when each
   * query remote tries to inform the other flatteners about its results: It will retry for 10s and if after 10s not all
   * flatteners did accept that information (which is bound to the request ID), the flatten will fail. It is required
   * that each query master retries, though, and therefore the next request of the query master should succeed.
   * 
   * <p>
   * And even more: When one flattening is completed and a different query later requests the same flattening, we re-use
   * most of the data of the old flattening (if that data is still valid, i.e. the flattened table was built based on
   * the same TableShards the source table currently has) - nevertheless we might now need to adjust the rowIds
   * differently than before (because any other query remote might flatten differently because it has more/less
   * TableShards loaded e.g.). For the latter case we use FlattenedTableUtil.
   */
  private class FlattenRunnable implements Runnable {
    private FlattenRequestDetails details;
    private UUID requestUuid;
    private Table table;
    private String tableName;
    private String flattenBy;
    private List<RNodeAddress> otherFlatteners;

    /* package */ FlattenRunnable(UUID requestUuid, FlattenRequestDetails details, Table table, String tableName,
        String flattenBy, List<RNodeAddress> otherFlatteners) {
      this.requestUuid = requestUuid;
      this.details = details;
      this.table = table;
      this.tableName = tableName;
      this.flattenBy = flattenBy;
      this.otherFlatteners = otherFlatteners;
    }

    @Override
    public void run() {
      long timeoutTime = System.nanoTime() + flattenTimeoutSeconds * 1_000_000_000L;
      requestIdByThreadId.put(Thread.currentThread().getId(), requestUuid);
      UUID flattenedTableId = requestUuid; // result UUID for the flattened table if we succeed.

      // fetch table shards in one go - otherwise they might change inside the table while we're processing them!
      List<TableShard> inputShardsSorted = table.getShards().stream()
          .sorted((s1, s2) -> Long.compare(s1.getLowestRowId(), s2.getLowestRowId())).collect(Collectors.toList());

      FlattenedTable flattenedTable =
          flattenManager.createFlattenedTable(table, inputShardsSorted, flattenBy, flattenedTableId);

      List<TableShard> flattenedShardsSorted = flattenedTable.getShards().stream()
          .sorted((s1, s2) -> Long.compare(s1.getLowestRowId(), s2.getLowestRowId())).collect(Collectors.toList());

      logger.trace(
          "Flattening '{}' by '{}', request ID {}, completed local computation, sending results to other flatteners.",
          tableName, flattenBy, requestUuid);

      // calculate the deltas in row-count that we need to distribute to other flatteners.
      Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRowsDelta = new HashMap<>();
      NavigableMap<Long, TableShard> origShardFirstRowIdToFlattenedShard = new TreeMap<>();
      for (int i = 0; i < inputShardsSorted.size(); i++) {
        origShardFirstRowIdToFlattenedNumberOfRowsDelta.put(inputShardsSorted.get(i).getLowestRowId(),
            flattenedShardsSorted.get(i).getNumberOfRowsInShard() - inputShardsSorted.get(i).getNumberOfRowsInShard());
        origShardFirstRowIdToFlattenedShard.put(inputShardsSorted.get(i).getLowestRowId(),
            flattenedShardsSorted.get(i));
      }

      for (RNodeAddress otherFlattener : otherFlatteners) {
        boolean retry = true;
        // retry for 10s. This is crucial to the overall algorithm, as the cluster computing the flattening might be
        // divided and processing different requestIds. This needs therefore to fail comparably quickly. And it will
        // fail if after some retries our request ID is still not accepted by the otherFlatteners. See comment of
        // runnable class.
        int retryCountLeft = 10;
        while (retry) {
          retry = false;
          try (ServiceProvider<ClusterFlattenService.Iface> serviceProv =
              connectionOrLocalHelper.getService(ClusterFlattenService.Iface.class, otherFlattener, null)) {

            serviceProv.getService().shardsFlattened(RUuidUtil.toRUuid(requestUuid),
                origShardFirstRowIdToFlattenedNumberOfRowsDelta, clusterManager.getOurNodeAddress().createRemote());
          } catch (RRetryLaterException e) {
            if (retryCountLeft == 0)
              // let the uncaughtExceptionHandler handle this...
              throw new RuntimeException(
                  "Exception while communicating with other flatteners (retry exhausted): " + e.getMessage(), e);

            logger.trace("Received a retry exception from {}: {}. Will retry.", otherFlattener, e.getMessage());
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

      // We flattened our shard. Before we can use it, though, we need to adjust the rowIds of it, since the
      // flattened table now still has the firstRowIds of the original table set, but the new table will (typically)
      // have more rows -> we have overlapping rowIds between tableShards. And this includes both local and remote table
      // shards -> we need to adjust the rowIds accordingly!

      // be sure to work on our local results, too.
      details.otherFlattenResults.add(origShardFirstRowIdToFlattenedNumberOfRowsDelta);

      int numberOfOtherFlattenersResponded = -1; // -1 because we put our own result in the deque in the line above.

      logger.trace("Flattening '{}' by '{}', request ID {}, waiting for results from other flatteners", tableName,
          flattenBy, requestUuid);

      while (numberOfOtherFlattenersResponded < otherFlatteners.size()) {
        synchronized (details.sync) {
          if (details.otherFlattenResults.isEmpty())
            try {
              details.sync.wait(1000); // 1s
            } catch (InterruptedException e) {
              // let the uncaughtExceptionHandler handle this...
              throw new RuntimeException("Interrupted while waiting for results of other nodes: " + e.getMessage(), e);
            }
        }

        if (System.nanoTime() > timeoutTime)
          throw new RuntimeException("Timed out waiting for other flatteners to calculate their result.");

        Map<Long, Long> otherFlattenerResult;
        while ((otherFlattenerResult = details.otherFlattenResults.poll()) != null) {
          logger.trace("Working on flattener result (limit): {}",
              Iterables.limit(otherFlattenerResult.entrySet(), 100));

          numberOfOtherFlattenersResponded++;
          for (Entry<Long, Long> otherEntry : otherFlattenerResult.entrySet()) {
            long otherOrigFirstRowId = otherEntry.getKey();
            long otherFlattenedNumerOfRowsDelta = otherEntry.getValue();

            Map<Long, TableShard> affectedShards =
                origShardFirstRowIdToFlattenedShard.tailMap(otherOrigFirstRowId, false);
            for (Entry<Long, TableShard> tableShardEntry : affectedShards.entrySet()) {
              logger.trace("Adjusting tableShard which was originally at rowId {}", tableShardEntry.getKey());

              for (StandardColumnShard colShard : tableShardEntry.getValue().getColumns().values()) {
                ((AdjustableStandardColumnShard) colShard)
                    .adjustToFirstRowId(colShard.getFirstRowId() + otherFlattenedNumerOfRowsDelta);
              }
            }
          }
        }
      }

      // Okay, all results from other flatteners received and incorporated, we're done!
      flattenedTableInstanceManager.registerFlattenedTableVersion(flattenedTableId, flattenedTable, tableName,
          flattenBy);

      // not in "finally", since we do not want to clear this here if we have an exception -> the
      // uncaughtExceptionHandler will handle that case!
      synchronized (details.sync) {
        requestIdByThreadId.remove(Thread.currentThread().getId());
        requestDetails.remove(requestUuid);
        currentFlattenRequest.remove(new Pair<>(tableName, flattenBy));
      }

      logger.info("Finished flattening '{}' by '{}', request ID {}.", tableName, flattenBy, requestUuid);

      for (Pair<RNodeAddress, UUID> resultPair : details.resultAddresses) {
        try (ServiceProvider<ClusterFlattenService.Iface> serviceProv =
            connectionOrLocalHelper.getService(ClusterFlattenService.Iface.class, resultPair.getLeft(), null)) {

          logger.trace("Sending result of flatten '{}' by '{}' to {} (its request ID was {})", tableName, flattenBy,
              resultPair.getLeft(), resultPair.getRight());

          serviceProv.getService().flattenDone(RUuidUtil.toRUuid(resultPair.getRight()),
              RUuidUtil.toRUuid(flattenedTableId), clusterManager.getOurNodeAddress().createRemote());
        } catch (Exception e) {
          logger.warn("Could not send flattening result {}/{} to requesting machine at {}. Ignoring.", requestUuid,
              resultPair.getRight(), resultPair.getLeft(), e);
        }
      }

      // At last, start triggering the computation of the metadata for the flattened table. We do this at last, since we
      // do not expect this to fail, since the original table was loaded successfully (and has valid metadata) and so
      // the merging etc for the flattened one should work well, too.
      try {
        metadataPublisher.publishMetadataOfTableShards(flattenedTable.getName(), flattenedTable.getShards());
      } catch (MergeImpossibleException e) {
        logger.error("Metadata of flattened table '{}' could not be computed.", e);
        // as we do not expect this to happen, just log and ignore.
      }
    }
  }

}