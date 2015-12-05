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
package org.diqube.server.queryremote.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.ConnectionException;
import org.diqube.connection.ConnectionOrLocalHelper;
import org.diqube.connection.ServiceProvider;
import org.diqube.connection.SocketListener;
import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.table.TableShard;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.executionenv.ExecutionEnvironment;
import org.diqube.executionenv.TableRegistry;
import org.diqube.executionenv.cache.ColumnShardCacheRegistry;
import org.diqube.executionenv.cache.WritableColumnShardCache;
import org.diqube.executionenv.querystats.QueryableColumnShard;
import org.diqube.function.IntermediaryResult;
import org.diqube.name.FlattenedTableNameGenerator;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryRegistry.QueryExceptionHandler;
import org.diqube.queries.QueryRegistry.QueryPercentHandler;
import org.diqube.queries.QueryRegistry.QueryResultHandler;
import org.diqube.queries.QueryStats;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuidProvider;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.cluster.RClusterQueryStatsUtil;
import org.diqube.remote.cluster.RIntermediateAggregationResultUtil;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.remote.cluster.thrift.RClusterQueryStatistics;
import org.diqube.remote.cluster.thrift.RExecutionException;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.ROldNewIntermediateAggregationResult;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.server.querymaster.query.QueryServiceHandler;
import org.diqube.server.queryremote.query.RemoteExecutionPlanExecutor.RemoteExecutionPlanExecutionCallback;
import org.diqube.server.util.ExecutablePlanQueryStatsUtil;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Implements {@link ClusterQueryService}, which is the cluster-side API to distribute the execution of queries.
 * 
 * This means that this service - in contrast to {@link QueryService} and its {@link QueryServiceHandler} - will not be
 * called by users (or the UI) directly.
 * 
 * When executing queries, the of this service methods will be called on the "query remote" nodes.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterQueryServiceHandler implements ClusterQueryService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryServiceHandler.class);

  /**
   * The executionUuids we are using by the queryUuids. THis contains only those executionUuids of queries which are
   * being executed currently. Additionally this map contains the "resultConnection" that was opened for the given
   * query.
   */
  private Map<UUID, Pair<UUID, ServiceProvider<ClusterQueryService.Iface>>> executionUuidsAndResultConnections =
      new ConcurrentHashMap<>();

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private ExecutablePlanFromRemoteBuilderFactory executablePlanBuilderFactory;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private QueryRegistry queryRegistry;

  @Inject
  private QueryUuidProvider queryUuidProvider;

  @Inject
  private ColumnShardCacheRegistry tableCacheRegistry;

  @Inject
  private ConnectionOrLocalHelper connectionOrLocalHelper;

  @Config(ConfigKey.CONCURRENT_TABLE_SHARD_EXECUTION_PER_QUERY)
  private int numberOfTableShardsToExecuteConcurrently;

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
    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);
    // The executionUuid we will use for the all executors executing something started by this API call.
    UUID executionUuid = queryUuidProvider.createNewExecutionUuid(queryUuid, "remote-" + queryUuid);

    SocketListener resultSocketListener = new SocketListener() {
      @Override
      public void connectionDied(String cause) {
        // Connection to result node died. The node will automatically be removed from ClusterManager and the
        // connection will automatically be handled.
        logger.error("Result node of query {} execution {} ({}) died unexpectedly. It will not receive any "
            + "results of the execution any more, cancelling execution.", queryUuid, executionUuid, resultAddress);

        executionUuidsAndResultConnections.remove(queryUuid);
        queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid); // this will kill our thread!
      }
    };

    ServiceProvider<ClusterQueryService.Iface> resultServiceProv;
    try {
      resultServiceProv =
          connectionOrLocalHelper.getService(ClusterQueryService.Iface.class, resultAddress, resultSocketListener);
    } catch (ConnectionException | InterruptedException e1) {
      logger.error("Could not open connection to the result node for query {} execution {} ({}). Will not start "
          + "executing anything.", queryUuid, executionUuid, resultAddress);
      return;
    }

    executionUuidsAndResultConnections.put(queryUuid, new Pair<>(executionUuid, resultServiceProv));

    RemoteExecutionPlanExecutor executor = new RemoteExecutionPlanExecutor(tableRegistry, executablePlanBuilderFactory,
        executorManager, queryRegistry, numberOfTableShardsToExecuteConcurrently);

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
            synchronized (resultServiceProv) {
              resultServiceProv.getService().executionException(remoteQueryUuid, ex);
            }
          } catch (TException e) {
            // swallow, the resultSocketListener handles this.
          }
        }

        // shutdown everything for all TableShards.
        synchronized (resultServiceProv) {
          try {
            resultServiceProv.close();
          } catch (IOException e) {
            logger.warn("Could not close connection");
          }
        }
        executionUuidsAndResultConnections.remove(queryUuid);
        queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid); // this will kill our thread!
      }
    };

    Holder<List<ExecutablePlan>> executablePlansHolder = new Holder<>();

    Pair<Runnable, List<ExecutablePlan>> prepareRes = null;
    try {
      prepareRes = executor.prepareExecution(queryUuid, executionUuid, executionPlan,
          new RemoteExecutionPlanExecutionCallback() {
            @Override
            public void newGroupIntermediaryAggregration(long groupId, String colName,
                ROldNewIntermediateAggregationResult result, short percentDone) {
              synchronized (resultServiceProv) {
                try {
                  resultServiceProv.getService().groupIntermediateAggregationResultAvailable(remoteQueryUuid, groupId,
                      colName, result, percentDone);
                } catch (TException e) {
                  logger.error("Could not send new group intermediaries to client for query {}", queryUuid, e);
                  exceptionHandler.handleException(null);
                }
              }
            }

            @Override
            public void newColumnValues(String colName, Map<Long, RValue> values, short percentDone) {
              synchronized (resultServiceProv) {
                try {
                  logger.trace("Constructed final column values, sending them now.");
                  resultServiceProv.getService().columnValueAvailable(remoteQueryUuid, colName, values, percentDone);
                } catch (TException e) {
                  logger.error("Could not send new group intermediaries to client for query {}", queryUuid, e);
                  exceptionHandler.handleException(null);
                }
              }
            }

            @Override
            public void executionDone() {
              // gather final stats
              queryRegistry.getOrCreateCurrentStatsManager().setCompletedNanos(System.nanoTime());

              for (ExecutablePlan plan : executablePlansHolder.getValue())
                new ExecutablePlanQueryStatsUtil().publishQueryStats(queryRegistry.getCurrentStatsManager(), plan);

              // send stats
              RClusterQueryStatistics remoteStats =
                  RClusterQueryStatsUtil.createRQueryStats(queryRegistry.getCurrentStatsManager().createQueryStats());
              logger.trace("Sending query statistics of {} to query master: {}", queryUuid, remoteStats);
              synchronized (resultServiceProv) {
                try {
                  resultServiceProv.getService().queryStatistics(remoteQueryUuid, remoteStats);
                } catch (TException e) {
                  logger.error("Could not send statistics to client for query {}", queryUuid, e);
                }

              }

              // update table cache with the results of this query execution. Note that if this query execution loaded
              // specific columns from the cache, they will be available in the ExecutionEnv as "temporary columns" -
              // and we will present them to the cache again right away. With this mechanism the cache can actively
              // count the usages of specific columns and therefore tune what it should cache and what not.
              String finalTableName;
              if (executionPlan.getFrom().isSetPlainTableName())
                finalTableName = executionPlan.getFrom().getPlainTableName();
              else {
                String origTableName = executionPlan.getFrom().getFlattened().getTableName();
                String flattenBy = executionPlan.getFrom().getFlattened().getFlattenBy();
                UUID flattenId = RUuidUtil.toUuid(executionPlan.getFrom().getFlattened().getFlattenId());
                finalTableName =
                    new FlattenedTableNameGenerator().createFlattenedTableName(origTableName, flattenBy, flattenId);
              }
              WritableColumnShardCache tableCache = tableCacheRegistry.getColumnShardCache(finalTableName);
              if (tableCache != null) {
                logger.info("Updating the table cache with results of query {}, execution {}", queryUuid,
                    executionUuid);
                for (ExecutablePlan plan : executablePlansHolder.getValue()) {
                  ExecutionEnvironment env = plan.getDefaultExecutionEnvironment();
                  Map<String, List<QueryableColumnShard>> tempColShards = env.getAllTemporaryColumnShards();
                  for (String colName : tempColShards.keySet()) {
                    ColumnShard tempCol = Iterables.getLast(tempColShards.get(colName)).getDelegate();
                    tableCache.offer(env.getFirstRowIdInShard(), tempCol.getName(), tempCol);
                  }
                }
              }

              synchronized (resultServiceProv) {
                try {
                  resultServiceProv.getService().executionDone(remoteQueryUuid);
                } catch (TException e) {
                  logger.error("Could not send 'done' to client for query {}", queryUuid, e);
                }
              }

              exceptionHandler.handleException(null);
            }
          });
    } catch (ExecutablePlanBuildException e) {
      // swallow, prepareRes == null, see just below.
    }

    if (prepareRes == null) {
      // we cannot execute anything, probably the TableShards were just unloaded.
      long now = System.nanoTime();
      QueryUuid.setCurrentQueryUuidAndExecutionUuid(queryUuid, executionUuid);
      try {
        queryRegistry.getOrCreateCurrentStatsManager().setStartedNanos(now);
        queryRegistry.getOrCreateCurrentStatsManager().setCompletedNanos(now);
        synchronized (resultServiceProv) {
          logger.info(
              "As there's nothing to execute for query {} execution {}, sending empty stats and an executionDone",
              queryUuid, executionUuid);
          resultServiceProv.getService().queryStatistics(remoteQueryUuid,
              RClusterQueryStatsUtil.createRQueryStats(queryRegistry.getCurrentStatsManager().createQueryStats()));
          resultServiceProv.getService().executionDone(remoteQueryUuid);
        }
      } finally {
        QueryUuid.clearCurrent();
      }
      exceptionHandler.handleException(null);
      return;
    }

    executablePlansHolder.setValue(prepareRes.getRight());

    // prepare to launch the execution in a different Thread
    Executor threadPool = executorManager.newQueryFixedThreadPoolWithTimeout(1,
        "query-remote-master-" + queryUuid + "-%d", queryUuid, executionUuid);
    queryRegistry.registerQueryExecution(queryUuid, executionUuid, exceptionHandler, false);

    // start execution of ExecutablePlan(s) asynchronously.
    queryRegistry.getOrCreateStatsManager(queryUuid, executionUuid).setStartedNanos(System.nanoTime());
    threadPool.execute(prepareRes.getLeft());
  }

  /**
   * New group intermediate aggregations are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void groupIntermediateAggregationResultAvailable(RUUID remoteQueryUuid, long groupId, String colName,
      ROldNewIntermediateAggregationResult result, short percentDoneDelta) throws TException {
    logger.trace("Received new group intermediary values in service. Constructing final objects to work on...");

    IntermediaryResult<Object, Object, Object> oldRes = null;
    if (result.isSetOldResult())
      oldRes = RIntermediateAggregationResultUtil.buildIntermediateAggregationResult(result.getOldResult());
    IntermediaryResult<Object, Object, Object> newRes = null;
    if (result.isSetNewResult())
      newRes = RIntermediateAggregationResultUtil.buildIntermediateAggregationResult(result.getNewResult());

    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);

    int numberOfPercentHandlers = 0;
    int numberOfResultHandlers = 0;

    for (QueryPercentHandler handler : queryRegistry.getQueryPercentHandlers(queryUuid)) {
      handler.newRemoteCompletionPercentDelta(percentDoneDelta);
      numberOfPercentHandlers++;
    }

    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(queryUuid)) {
      handler.newIntermediaryAggregationResult(groupId, colName, oldRes, newRes);
      numberOfResultHandlers++;
    }

    logger.trace("Informed {} result and {} percent handlers about new column values for query {}",
        numberOfPercentHandlers, numberOfResultHandlers, queryUuid);
  }

  /**
   * New column values are available for a given queryId.
   * 
   * This method will be called as result from calling
   * {@link #executeOnAllShards(RExecutionPlan, RUUID, RNodeAddress, boolean)} on another node.
   */
  @Override
  public void columnValueAvailable(RUUID remoteQueryUuid, String colName, Map<Long, RValue> valuesByRowId,
      short percentDoneDelta) throws TException {
    logger.trace("Received new column values in service. Constructing final objects to work on...");
    Map<Long, Object> values = new HashMap<>();
    for (Entry<Long, RValue> remoteEntry : valuesByRowId.entrySet())
      values.put(remoteEntry.getKey(), RValueUtil.createValue(remoteEntry.getValue()));

    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);

    int numberOfPercentHandlers = 0;
    int numberOfResultHandlers = 0;

    for (QueryPercentHandler handler : queryRegistry.getQueryPercentHandlers(queryUuid)) {
      handler.newRemoteCompletionPercentDelta(percentDoneDelta);
      numberOfPercentHandlers++;
    }

    for (QueryResultHandler handler : queryRegistry.getQueryResultHandlers(queryUuid)) {
      handler.newColumnValues(colName, values);
      numberOfResultHandlers++;
    }

    logger.trace("Informed {} result and {} percent handlers about new column values for query {}",
        numberOfPercentHandlers, numberOfResultHandlers, queryUuid);
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
   * We are asked to cancel the execution of the given query.
   */
  @Override
  public void cancelExecution(RUUID remoteQueryUuid) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);
    Pair<UUID, ServiceProvider<ClusterQueryService.Iface>> p = executionUuidsAndResultConnections.get(queryUuid);
    if (p != null) {
      UUID executionUuid = p.getLeft();
      logger.info("We were asked to cancel execution of query {} which has exection {}. Doing that.", queryUuid,
          executionUuid);

      if (p.getRight() != null)
        try {
          p.getRight().close();
        } catch (IOException e) {
          logger.warn("Could not close connection.");
        }

      executionUuidsAndResultConnections.remove(queryUuid);
      queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
      executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid); // this will kill our thread!
    }
  }

  @Override
  public void queryStatistics(RUUID remoteQueryUuid, RClusterQueryStatistics remoteStats) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(remoteQueryUuid);
    QueryStats stats = RClusterQueryStatsUtil.createQueryStats(remoteStats);

    queryRegistry.remoteQueryStatsAvailable(queryUuid, stats);
  }

}