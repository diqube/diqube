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
import org.diqube.remote.cluster.ClusterFlatteningServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterFlatteningService;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
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

  private Map<UUID, Deque<UUID>> requestToFlattenedTableId = new ConcurrentHashMap<>();
  private Map<UUID, String> requestToException = new ConcurrentHashMap<>();
  private Map<UUID, Object> requestToSync = new ConcurrentHashMap<>();

  /**
   * Ensures the flattened table is available on all cluster nodes serving the table and triggers flattening if needed.
   * 
   * @param table
   *          The name of the table to be flattened.
   * @param flattenBy
   *          The "flatten by" field, see {@link FlattenUtil} for details.
   * @return Pair of UUID and list. List is list of nodes that have the flattened table upon return of this method. The
   *         UUID is the flatten ID to be used. If <code>null</code> is returned, the corresponding table does not have
   *         any nodes serving it.
   */
  public Pair<UUID, List<RNodeAddress>> flatten(String table, String flattenBy)
      throws FlattenException, InterruptedException {
    logger.info("Identified that a flattened version of '{}' is needed (by '{}').", table, flattenBy);

    long timeoutTime = System.nanoTime() + flattenTimeoutSeconds * 1_000_000_000;

    boolean firstRun = true;
    while (true) {
      if (firstRun)
        firstRun = false;
      else {
        Thread.sleep(1000); // 1s

        if (System.nanoTime() > timeoutTime)
          throw new FlattenException("Timed out flattening table '" + table + "' by '" + flattenBy + "'");

        logger.info("Retrying to find flattening state of cluster for table '{}', flatten by '{}'", table, flattenBy);
      }

      Collection<RNodeAddress> nodesServingTable = clusterManager.getClusterLayout().findNodesServingTable(table);

      if (nodesServingTable.isEmpty())
        return null;

      Set<UUID> validFlattenUuids = new HashSet<>();
      boolean error = false;
      for (RNodeAddress node : nodesServingTable) {
        try (ServiceProvider<ClusterFlatteningService.Iface> serviceProv =
            connectionOrLocalHelper.getService(ClusterFlatteningService.Client.class,
                ClusterFlatteningService.Iface.class, ClusterFlatteningServiceConstants.SERVICE_NAME, node, null)) {

          ROptionalUuid nodeRes = serviceProv.getService().getLatestValidFlattening(table, flattenBy);
          validFlattenUuids.add(nodeRes.isSetUuid() ? RUuidUtil.toUuid(nodeRes.getUuid()) : null);
        } catch (ConnectionException | IOException | IllegalStateException | TException e) {
          logger.info("Exception while talking to {} about flattening table {}. Will retry.", node, table, e);
          error = true;
          break;
        }
      }
      if (error)
        continue;

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

        try {
          error = false;
          for (RNodeAddress node : nodesServingTable) {
            try (ServiceProvider<ClusterFlatteningService.Iface> serviceProv =
                connectionOrLocalHelper.getService(ClusterFlatteningService.Client.class,
                    ClusterFlatteningService.Iface.class, ClusterFlatteningServiceConstants.SERVICE_NAME, node, null)) {

              serviceProv.getService().flattenAllLocalShards(flattenRequestRuuid, table, flattenBy,
                  new ArrayList<>(nodesServingTable), clusterManager.getOurHostAddr().createRemote());
            } catch (ConnectionException | IOException | IllegalStateException | TException e) {
              logger.info("Exception while talking to {} about flattening table {}. Will retry.", node, table, e);
              error = true;
              break;
            }
          }

          if (error)
            continue;

          logger.info("Waiting for remotes to flatten request {}", flattenRequestUuid);

          int numberOfRemotesDone = 0;
          UUID finalFlattenedTableId = null;
          while (numberOfRemotesDone < nodesServingTable.size()) {
            synchronized (sync) {
              if (requestToFlattenedTableId.get(flattenRequestUuid).isEmpty()
                  && requestToException.get(flattenRequestUuid) == null)
                sync.wait(1000); // 1s
            }

            if (requestToException.get(flattenRequestUuid) != null)
              throw new FlattenException(
                  "Flatten exception from remote: " + requestToException.get(flattenRequestUuid));

            while (!requestToFlattenedTableId.get(flattenRequestUuid).isEmpty()) {
              UUID flattenedTableId = requestToFlattenedTableId.get(flattenRequestUuid).pop();
              numberOfRemotesDone++;

              if (finalFlattenedTableId == null)
                finalFlattenedTableId = flattenedTableId;

              if (!finalFlattenedTableId.equals(flattenedTableId)) {
                // remotes responded with different flattened table IDs. Should never happen, but to be sure...
                error = true;
                break;
              }
            }
            if (error)
              break;

            if (System.nanoTime() > timeoutTime)
              throw new FlattenException("Timed out waiting for results from remotes when trying to flatten '" + table
                  + "' with request " + flattenRequestUuid);
          }
          if (error)
            continue;

          logger.info("Flatten request {} finished successfully. Flattened '{}' by '{}' with result flatten ID {}",
              flattenRequestUuid, table, flattenBy, finalFlattenedTableId);

          // okay, flattening finished.
          return new Pair<UUID, List<RNodeAddress>>(finalFlattenedTableId, new ArrayList<>(nodesServingTable));
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

        return new Pair<UUID, List<RNodeAddress>>(flattenId, new ArrayList<>(nodesServingTable));
      }
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

}
