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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.ConnectionPool.Connection;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.TableShard;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.TableRegistry;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryRegistry.QueryExceptionHandler;
import org.diqube.queries.QueryRegistry.QueryResultHandler;
import org.diqube.queries.QueryUuidProvider;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.cluster.ClusterNodeServiceConstants;
import org.diqube.remote.cluster.RIntermediateAggregationResultUtil;
import org.diqube.remote.cluster.thrift.ClusterNodeService;
import org.diqube.remote.cluster.thrift.ClusterNodeService.Iface;
import org.diqube.remote.cluster.thrift.RExecutionException;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.ROldNewIntermediateAggregationResult;
import org.diqube.remote.query.thrift.QueryService;
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
 * When executing queries, the of this service methods will be called on the "query remote" nodes.
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

  @Inject
  private QueryUuidProvider queryUuidProvider;

  @Inject
  private ClusterManager clusterManager;

  /**
   * Starts executing a {@link RExecutionPlan} on all {@link TableShard}s on this node, which act as "query remote"
   * node.
   * 
   * Please note that the results of this call will be available through a {@link QueryResultHandler} which can be
   * registered at {@link QueryRegistry}.
   */
  @Override
  public void executeOnAllLocalShards(RExecutionPlan executionPlan, RUUID remoteQueryUuid, RNodeAddress resultAddress)
      throws TException {
    Connection<ClusterNodeService.Client> resultConnection = connectionPool
        .reserveConnection(ClusterNodeService.Client.class, ClusterNodeServiceConstants.SERVICE_NAME, resultAddress);

    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);
    // The executionUuid we will use for the all executors executing something started by this API call.
    UUID executionUuid = queryUuidProvider.createNewExecutionUuid(queryUuid, "remote-" + queryUuid);

    RemoteExecutionPlanExecutor executor =
        new RemoteExecutionPlanExecutor(tableRegistry, executablePlanBuilderFactory, executorManager);

    // Exception handler that handles exceptions that are thrown during execution (one handler for all TableShards!)
    // This can also close all resources, if parameters "null,null" are passed.
    QueryExceptionHandler exceptionHandler = new QueryExceptionHandler() {
      @Override
      public void handleException(Throwable t) {
        if (t != null) {
          logger.error("Exception while executing query {} execution {}", queryUuid, executionUuid, t);
          RExecutionException ex = new RExecutionException();
          ex.setMessage(t.getMessage());
          try {
            resultConnection.getService().executionException(remoteQueryUuid, ex);
          } catch (TException e) {
            logger.error("Could not sent new group intermediaries to client for query {}", queryUuid, e);
            // TODO #32 mark connection as dead.
          }
        }

        // shutdown everything for all TableShards.
        synchronized (resultConnection) {
          connectionPool.releaseConnection(resultConnection);
        }
        queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid); // this will kill our
                                                                                      // thread!
      }
    };

    Runnable execute =
        executor.prepareExecution(queryUuid, executionUuid, executionPlan, new RemoteExecutionPlanExecutionCallback() {
          @Override
          public void newGroupIntermediaryAggregration(long groupId, String colName,
              ROldNewIntermediateAggregationResult result) {
            synchronized (resultConnection) {
              try {
                resultConnection.getService().groupIntermediateAggregationResultAvailable(remoteQueryUuid, groupId,
                    colName, result);
              } catch (TException e) {
                logger.error("Could not sent new group intermediaries to client for query {}", queryUuid, e);

                // TODO #32 mark connection as dead.
                exceptionHandler.handleException(null);
              }
            }
          }

          @Override
          public void newColumnValues(String colName, Map<Long, RValue> values) {
            synchronized (resultConnection) {
              try {
                resultConnection.getService().columnValueAvailable(remoteQueryUuid, colName, values);
              } catch (TException e) {
                logger.error("Could not sent new group intermediaries to client for query {}", queryUuid, e);

                // TODO #32 mark connection as dead.
                exceptionHandler.handleException(null);
              }
            }
          }

          @Override
          public void executionDone() {
            synchronized (resultConnection) {
              try {
                resultConnection.getService().executionDone(remoteQueryUuid);
              } catch (TException e) {
                logger.error("Could not sent 'done' to client for query {}", queryUuid, e);
                // TODO #32 mark connection as dead.
              }
            }
            exceptionHandler.handleException(null);
          }

          @Override
          public void exceptionThrown(Throwable t) {
            logger.error("Exception while executing query {}", queryUuid, t);
            RExecutionException ex = new RExecutionException(t.getMessage());
            synchronized (resultConnection) {
              try {
                resultConnection.getService().executionException(remoteQueryUuid, ex);
              } catch (TException e) {
                logger.error("Could not sent 'exception' to client for query {}", queryUuid, e);
                // TODO #32 mark connection as dead.
              }
            }
            exceptionHandler.handleException(null);
          }
        });

    // prepare to launch the execution in a different Thread
    Executor threadPool = executorManager.newQueryFixedThreadPool(1, "query-remote-master-" + queryUuid + "-%d",
        queryUuid, executionUuid);
    queryRegistry.registerQueryExecution(queryUuid, executionUuid, exceptionHandler);

    // start execution of ExecutablePlan(s) asynchronously.
    threadPool.execute(execute);
  }

  /**
   * New group intermediate aggregations are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void groupIntermediateAggregationResultAvailable(RUUID remoteQueryUuid, long groupId, String colName,
      ROldNewIntermediateAggregationResult result) throws TException {

    IntermediaryResult<Object, Object, Object> oldRes = null;
    if (result.isSetOldResult())
      oldRes = RIntermediateAggregationResultUtil.buildIntermediateAggregationResult(result.getOldResult());
    IntermediaryResult<Object, Object, Object> newRes = null;
    if (result.isSetNewResult())
      newRes = RIntermediateAggregationResultUtil.buildIntermediateAggregationResult(result.getNewResult());

    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(RUuidUtil.toUuid(remoteQueryUuid)))
      handler.newIntermediaryAggregationResult(groupId, colName, oldRes, newRes);
  }

  /**
   * New column values are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void columnValueAvailable(RUUID remoteQueryUuid, String colName, Map<Long, RValue> valuesByRowId)
      throws TException {
    Map<Long, Object> values = new HashMap<>();
    for (Entry<Long, RValue> remoteEntry : valuesByRowId.entrySet())
      values.put(remoteEntry.getKey(), RValueUtil.createValue(remoteEntry.getValue()));

    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(RUuidUtil.toUuid(remoteQueryUuid)))
      handler.newColumnValues(colName, values);
  }

  /**
   * An execution that was started with {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} was
   * completed successfully.
   */
  @Override
  public void executionDone(RUUID remoteQueryUuid) throws TException {
    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(RUuidUtil.toUuid(remoteQueryUuid)))
      handler.oneRemoteDone();
  }

  /**
   * An execution that was started with {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} ended
   * with an exception.
   */
  @Override
  public void executionException(RUUID remoteQueryUuid, RExecutionException executionException) throws TException {
    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(RUuidUtil.toUuid(remoteQueryUuid)))
      handler.oneRemoteException(executionException.getMessage());
  }

  /**
   * A new cluster node says "hello".
   */
  @Override
  public void hello(RNodeAddress newNode) throws TException {
    clusterManager.newNode(newNode);
  }

  /**
   * Someone asks us what cluster nodes we know and what tables they serve shards of.
   */
  @Override
  public Map<RNodeAddress, Map<Long, List<String>>> clusterLayout() throws TException {
    return clusterManager.getClusterLayout().createRemoteLayout();
  }

  /**
   * A cluster node has an updated list of tables available for which it serves data.
   */
  @Override
  public void newNodeData(RNodeAddress nodeAddr, long version, List<String> tables) throws TException {
    clusterManager.loadNodeInfo(nodeAddr, version, tables);
  }

  /**
   * A cluster node died.
   */
  @Override
  public void nodeDied(RNodeAddress nodeAddr) throws TException {
    clusterManager.nodeDied(nodeAddr);
  }

}
