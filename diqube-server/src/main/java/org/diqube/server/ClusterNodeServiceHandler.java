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
package org.diqube.server;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.TableShard;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.TableRegistry;
import org.diqube.queries.QueryRegistry;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.thrift.ClusterNodeService;
import org.diqube.remote.cluster.thrift.ClusterNodeService.Iface;
import org.diqube.remote.cluster.thrift.RExecutionException;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.ROldNewIntermediateAggregationResult;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.server.ConnectionPool.Connection;
import org.diqube.server.RemoteExecutionPlanExecutor.RemoteExecutionPlanExecutionCallback;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements {@link ClusterNodeService}, which manages communication between various cluster nodes of diqube servers.
 * 
 * This means that this service - in contrast to {@link QueryService} and its {@link QueryServiceHandler} - will not be
 * called by users (or the UI) directly.
 * 
 * When executing queries, these methods will be called on the "query remote" nodes.
 * 
 * TODO #11 implement.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterNodeServiceHandler implements Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterNodeServiceHandler.class);

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private ExecutablePlanFromRemoteBuilderFactory executablePlanBuilderFactory;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private ConnectionPool connectionPool;

  @Inject
  private QueryRegistry queryRegistry;

  /**
   * Starts executing a {@link RExecutionPlan} on all {@link TableShard}s on this node, which act as "query remote"
   * node.
   */
  @Override
  public void executeOnAllLocalShards(RExecutionPlan executionPlan, RUUID remoteQueryId, RNodeAddress resultAddress)
      throws TException {
    Connection<ClusterNodeService.Client> connection =
        connectionPool.reserveConnection(ClusterNodeService.Client.class, resultAddress);

    UUID queryUuid = RUuidUtil.toUuid(remoteQueryId);

    RemoteExecutionPlanExecutor executor =
        new RemoteExecutionPlanExecutor(tableRegistry, executablePlanBuilderFactory, executorManager);

    Runnable execute = executor.prepareExecution(queryUuid, executionPlan, new RemoteExecutionPlanExecutionCallback() {

      private void returnResources() {
        connectionPool.releaseConnection(connection);
        queryRegistry.unregisterQuery(queryUuid);
        executorManager.shutdownEverythingOfQuery(queryUuid); // this will kill our thread, too!
      }

      @Override
      public void newGroupIntermediaryAggregration(long groupId, String colName,
          ROldNewIntermediateAggregationResult result) {
        synchronized (connection) {
          try {
            connection.getService().groupIntermediateAggregationResultAvailable(remoteQueryId, groupId, colName,
                result);
          } catch (TException e) {
            logger.error("Could not sent new group intermediaries to client for query {}", queryUuid, e);

            // TODO #32 mark connection as dead.
            returnResources();
          }
        }
      }

      @Override
      public void newColumnValues(String colName, Map<Long, RValue> values) {
        synchronized (connection) {
          try {
            connection.getService().columnValueAvailable(remoteQueryId, colName, values);
          } catch (TException e) {
            logger.error("Could not sent new group intermediaries to client for query {}", queryUuid, e);

            // TODO #32 mark connection as dead.
            returnResources();
          }
        }
      }

      @Override
      public void executionDone() {
        synchronized (connection) {
          try {
            connection.getService().executionDone(remoteQueryId);
          } catch (TException e) {
            logger.error("Could not sent 'done' to client for query {}", queryUuid, e);
            // TODO #32 mark connection as dead.
          }
        }
        returnResources();
      }

      @Override
      public void exceptionThrown(Throwable t) {
        logger.error("Exception while executing query {}", queryUuid, t);
        RExecutionException ex = new RExecutionException(t.getMessage());
        synchronized (connection) {
          try {
            connection.getService().executionException(remoteQueryId, ex);
          } catch (TException e) {
            logger.error("Could not sent 'exception' to client for query {}", queryUuid, e);
            // TODO #32 mark connection as dead.
          }
        }
        returnResources();
      }
    });

    Executor threadPool =
        executorManager.newQueryFixedThreadPool(1, "query-remote-master-" + queryUuid + "-%d", queryUuid);
    threadPool.execute(execute);
  }

  /**
   * New group intermediate aggregations are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void groupIntermediateAggregationResultAvailable(RUUID queryId, long groupId, String colName,
      ROldNewIntermediateAggregationResult result) throws TException {
  }

  /**
   * New column values are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void columnValueAvailable(RUUID queryId, String colName, Map<Long, RValue> valuesByRowId) throws TException {
  }

  /**
   * An execution that was started with {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} was
   * completed successfully.
   */
  @Override
  public void executionDone(RUUID queryId) throws TException {
  }

  /**
   * An execution that was started with {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} ended
   * with an exception.
   */
  @Override
  public void executionException(RUUID queryId, RExecutionException executionException) throws TException {
  }

}
