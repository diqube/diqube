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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.connection.ConnectionException;
import org.diqube.cluster.connection.ConnectionOrLocalHelper;
import org.diqube.cluster.connection.ServiceProvider;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.ClusterFlattenServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that manages flattening tables for the query master.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryMasterFlattenService {
  private static final Logger logger = LoggerFactory.getLogger(QueryMasterFlattenService.class);

  @Inject
  private ClusterManager clusterManager;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Config(ConfigKey.FLATTEN_TIMEOUT_SECONDS)
  private int flattenTimeoutSeconds;

  @Inject
  private ExecutorManager executorManager;

  private ExecutorService flattenExecutor;

  private Map<UUID, Deque<UUID>> requestToFlattenedTableId = new ConcurrentHashMap<>();
  private Map<UUID, String> requestToException = new ConcurrentHashMap<>();
  private Map<UUID, Object> requestToSync = new ConcurrentHashMap<>();
  /** UUID may be <code>null</code> */
  private Map<Long, Pair<UUID, QueryMasterFlattenCallback>> threadIdToRequestUuidAndCallback =
      new ConcurrentHashMap<>();

  @PostConstruct
  public void initialize() {
    flattenExecutor = executorManager.newCachedThreadPoolWithMax("master-flatten-%d", new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        Pair<UUID, QueryMasterFlattenCallback> uuidAndCallback = threadIdToRequestUuidAndCallback.remove(t.getId());
        if (uuidAndCallback != null) {
          uuidAndCallback.getRight().flattenException("Uncaught exception while flattening.", e);

          if (uuidAndCallback.getLeft() != null) {
            requestToFlattenedTableId.remove(uuidAndCallback.getLeft());
            requestToException.remove(uuidAndCallback.getLeft());
            requestToSync.remove(uuidAndCallback.getLeft());
          }
        }
      }
    }, 10);
  }

  /**
   * Ensures the flattened table is available on all cluster nodes serving the table and triggers flattening if needed.
   * 
   * <p>
   * This will automatically retry the flattening if any problems occur, but will timeout approximately after
   * {@link ConfigKey#FLATTEN_TIMEOUT_SECONDS} seconds.
   * 
   * @param table
   *          The name of the table to be flattened.
   * @param flattenBy
   *          The "flatten by" field, see {@link Flattener} for details.
   * @param callback
   *          The callback that will be informed about the result of the flattening.
   */
  public void flattenAsync(String table, String flattenBy, QueryMasterFlattenCallback callback) {
    logger.info("Requested a flattened version of '{}' by '{}'.", table, flattenBy);

    long timeoutTime = System.nanoTime() + flattenTimeoutSeconds * 1_000_000_000L;

    Runnable flattenRunnable = new Runnable() {
      @Override
      public void run() {
        // remember the callback if an uncaught exception occurs. No UUID yet, there's nothing to cleanup in the
        // UUID-based maps.
        // Note that threadIdToRequestUuidAndCallback is NOT cleaned up in a try..finally, since the uncaught exception
        // handler needs access to that map!
        threadIdToRequestUuidAndCallback.put(Thread.currentThread().getId(), new Pair<>(null, callback));

        Collection<RNodeAddress> nodesServingTable = clusterManager.getClusterLayout().findNodesServingTable(table);

        if (nodesServingTable.isEmpty()) {
          threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
          callback.noNodesServingOriginalTable();
          return;
        }

        Set<UUID> validFlattenUuids = new HashSet<>();
        for (RNodeAddress node : nodesServingTable) {
          try (ServiceProvider<ClusterFlattenService.Iface> serviceProv =
              connectionOrLocalHelper.getService(ClusterFlattenService.Client.class, ClusterFlattenService.Iface.class,
                  ClusterFlattenServiceConstants.SERVICE_NAME, node, null)) {

            ROptionalUuid nodeRes = serviceProv.getService().getLatestValidFlattening(table, flattenBy);
            validFlattenUuids.add(nodeRes.isSetUuid() ? RUuidUtil.toUuid(nodeRes.getUuid()) : null);
          } catch (ConnectionException | IOException | IllegalStateException | TException e) {
            logger.info("Exception while talking to {} about flattening table {}. Will retry.", node, table, e);
            threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
            scheduleRetry();
            return;
          } catch (InterruptedException e) {
            threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
            callback.flattenException("Interrupted", e);
            return;
          }
        }

        if (validFlattenUuids.size() > 1 || validFlattenUuids.iterator().next() == null) {
          // we need to re-flatten, as the cluster nodes returned multiple flatten UUIDs or all replied with null.
          UUID flattenRequestUuid = UUID.randomUUID();
          RUUID flattenRequestRuuid = RUuidUtil.toRUuid(flattenRequestUuid);

          logger.info("Triggering the flattening of '{}' by '{}'. New flatten request ID {}.", table, flattenBy,
              flattenRequestUuid);

          Object sync = new Object();
          requestToSync.put(flattenRequestUuid, sync);
          requestToException.remove(flattenRequestUuid);
          requestToFlattenedTableId.put(flattenRequestUuid, new ConcurrentLinkedDeque<>());

          // we now initialized the UUID-keyed maps, make sure the uncaught exception handler will cleanup them, too.
          threadIdToRequestUuidAndCallback.put(Thread.currentThread().getId(),
              new Pair<>(flattenRequestUuid, callback));

          try {
            for (RNodeAddress node : nodesServingTable) {
              List<RNodeAddress> otherFlatteners =
                  nodesServingTable.stream().filter(n -> n != node).collect(Collectors.toList());
              try (ServiceProvider<ClusterFlattenService.Iface> serviceProv =
                  connectionOrLocalHelper.getService(ClusterFlattenService.Client.class,
                      ClusterFlattenService.Iface.class, ClusterFlattenServiceConstants.SERVICE_NAME, node, null)) {

                serviceProv.getService().flattenAllLocalShards(flattenRequestRuuid, table, flattenBy, otherFlatteners,
                    clusterManager.getOurHostAddr().createRemote());
              } catch (ConnectionException | IOException | IllegalStateException | TException e) {
                logger.info("Exception while talking to {} about flattening table {}. Will retry.", node, table, e);
                threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                scheduleRetry();
                return;
              } catch (InterruptedException e) {
                threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                callback.flattenException("Interrupted", e);
                return;
              }
            }

            logger.info("Waiting for remotes to flatten request {}", flattenRequestUuid);

            int numberOfRemotesDone = 0;
            UUID finalFlattenedTableId = null;
            while (numberOfRemotesDone < nodesServingTable.size()) {
              synchronized (sync) {
                if (requestToFlattenedTableId.get(flattenRequestUuid).isEmpty()
                    && requestToException.get(flattenRequestUuid) == null)
                  try {
                    sync.wait(1000);// 1s
                  } catch (InterruptedException e) {
                    threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                    callback.flattenException("Interrupted", e);
                    return;
                  }
              }

              if (requestToException.get(flattenRequestUuid) != null) {
                threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                callback.flattenException(
                    "Flatten exception from remote: " + requestToException.get(flattenRequestUuid), null);
                return;
              }

              while (!requestToFlattenedTableId.get(flattenRequestUuid).isEmpty()) {
                UUID flattenedTableId = requestToFlattenedTableId.get(flattenRequestUuid).pop();
                numberOfRemotesDone++;

                if (finalFlattenedTableId == null)
                  finalFlattenedTableId = flattenedTableId;

                if (!finalFlattenedTableId.equals(flattenedTableId)) {
                  // remotes responded with different flattened table IDs. Should never happen, but to be sure...
                  threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                  scheduleRetry();
                  return;
                }
              }

              if (System.nanoTime() > timeoutTime) {
                threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
                callback.flattenException("Timed out waiting for results from remotes when trying to flatten '" + table
                    + "' with request " + flattenRequestUuid, null);
                return;
              }
            }

            logger.info("Flatten request {} finished successfully. Flattened '{}' by '{}' with result flatten ID {}",
                flattenRequestUuid, table, flattenBy, finalFlattenedTableId);

            // okay, flattening finished.
            threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
            callback.flattenComplete(finalFlattenedTableId, new ArrayList<>(nodesServingTable));
            return;
          } finally {
            requestToSync.remove(flattenRequestUuid);
            requestToException.remove(flattenRequestUuid);
            requestToFlattenedTableId.remove(flattenRequestUuid);
          }
        } else {
          // all nodes returned the same flatten ID as valid, so we'll use that node set and that flatten ID.
          UUID flattenId = validFlattenUuids.iterator().next();
          logger.info(
              "Found that all cluster nodes aggree on flattening ID {} for flattening table '{}' by '{}'. Using that.",
              flattenId, table, flattenBy);

          threadIdToRequestUuidAndCallback.remove(Thread.currentThread().getId());
          callback.flattenComplete(flattenId, new ArrayList<>(nodesServingTable));
          return;
        }
      }

      private void scheduleRetry() {
        try {
          Thread.sleep(1000);// 1s
        } catch (InterruptedException e) {
          callback.flattenException("Interrupted", e);
          return;
        }

        if (System.nanoTime() > timeoutTime) {
          callback.flattenException("Timed out flattening table '" + table + "' by '" + flattenBy + "'", null);
          return;
        }

        logger.info("Retrying to find flattening state of cluster for table '{}', flatten by '{}'", table, flattenBy);
        run();
      }
    };

    flattenExecutor.execute(flattenRunnable);
  }

  /**
   * Flattens a table in the cluster, just like {@link #flattenAsync(String, String, QueryMasterFlattenCallback)}, but
   * synchronous.
   * 
   * @param table
   *          The name of the table to be flattened.
   * @param flattenBy
   *          The "flatten by" field, see {@link Flattener} for details.
   * @return Pair of UUID and list. List is list of nodes that have the flattened table upon return of this method. The
   *         UUID is the flatten ID to be used. If <code>null</code> is returned, the corresponding table does not have
   *         any nodes serving it.
   */
  public Pair<UUID, List<RNodeAddress>> flatten(String table, String flattenBy)
      throws FlattenException, InterruptedException {
    Holder<Pair<UUID, List<RNodeAddress>>> res = new Holder<>();
    Holder<String> exceptionMsg = new Holder<>();
    Holder<Throwable> exceptionCause = new Holder<>();
    Object sync = new Object();

    flattenAsync(table, flattenBy, new QueryMasterFlattenCallback() {
      @Override
      public void noNodesServingOriginalTable() {
        synchronized (sync) {
          exceptionMsg.setValue("No nodes serving table " + table);
          exceptionCause.setValue(null);
          sync.notifyAll();
        }
      }

      @Override
      public void flattenException(String msg, Throwable cause) {
        synchronized (sync) {
          exceptionMsg.setValue("Exception while flattening " + table);
          exceptionCause.setValue(cause);
          sync.notifyAll();
        }
      }

      @Override
      public void flattenComplete(UUID flattenId, List<RNodeAddress> nodes) {
        synchronized (sync) {
          res.setValue(new Pair<>(flattenId, nodes));
          sync.notifyAll();
        }
      }
    });

    synchronized (sync) {
      sync.wait();

      if (exceptionMsg.getValue() != null || exceptionCause.getValue() != null) {
        if (exceptionCause.getValue() instanceof InterruptedException)
          throw (InterruptedException) exceptionCause.getValue();

        throw new FlattenException(exceptionMsg.getValue(), exceptionCause.getValue());
      }

      return res.getValue();
    }
  }

  public void singleRemoteCompletedFlattening(UUID flattenRequestId, UUID flattenedTableId, RNodeAddress node) {
    Deque<UUID> deque = requestToFlattenedTableId.get(flattenRequestId);
    Object sync = requestToSync.get(flattenRequestId);
    if (deque == null || sync == null)
      // we received an update on something that we cleaned up already. ignore.
      return;

    synchronized (sync) {
      deque.push(flattenedTableId);
      sync.notifyAll();
    }
  }

  public void singleRemoteFailedFlattening(UUID flattenRequestId, String msg) {
    Object sync = requestToSync.get(flattenRequestId);
    if (sync == null)
      // we received an update on something that we cleaned up already. ignore.
      return;

    synchronized (sync) {
      requestToException.put(flattenRequestId, (msg != null) ? msg : "null");
      sync.notifyAll();
    }
  }

  /**
   * Something went wrong while flattening.
   */
  public static class FlattenException extends Exception {
    private static final long serialVersionUID = 1L;

    /* package */ FlattenException(String msg) {
      super(msg);
    }

    /* package */ FlattenException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Callback for the {@link QueryMasterFlattenService#flattenAsync(String, String, QueryMasterFlattenCallback)}.
   */
  public static interface QueryMasterFlattenCallback {
    /**
     * It was found that no cluster nodes serve the original table.
     */
    public void noNodesServingOriginalTable();

    /**
     * Flatten completed.
     * 
     * @param flattenId
     *          The result flattenId that can be used on the given nodes.
     * @param nodes
     *          The nodes that the table was flattened on.
     */
    public void flattenComplete(UUID flattenId, List<RNodeAddress> nodes);

    /**
     * Exception during flattening.
     * 
     * @param cause
     *          may be <code>null</code>.
     */
    public void flattenException(String msg, Throwable cause);
  }

}
