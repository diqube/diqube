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
package org.diqube.server.querymaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.connection.Connection;
import org.diqube.cluster.connection.ConnectionException;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.SocketListener;
import org.diqube.context.AutoInstatiate;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.exception.ValidationException;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryRegistry.QueryExceptionHandler;
import org.diqube.queries.QueryStats;
import org.diqube.queries.QueryUuidProvider;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.ClusterQueryServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.query.QueryResultServiceConstants;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.QueryService.Iface;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;
import org.diqube.server.util.ExecutablePlanQueryStatsUtil;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Holder;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements logic of a {@link QueryService}, which is the service that is called on a "Query master" node to execute
 * queries.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryServiceHandler implements Iface {

  private static final Logger logger = LoggerFactory.getLogger(QueryServiceHandler.class);

  @Inject
  private ExecutionPlanBuilderFactory executionPlanBuilderFactory;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private QueryRegistry queryRegistry;

  @Inject
  private ConnectionPool connectionPool;

  @Inject
  private QueryUuidProvider queryUuidProvider;

  /**
   * Executes the given diql query on the diqube cluster, where this node will be the query master.
   * 
   * The query is executed synchronously and the result will be provided as return value.
   */
  @Override
  public RResultTable syncExecuteQuery(String diql) throws RQueryException, TException {
    UUID queryUuid = UUID.randomUUID();
    logger.info("Sync query {}: {}", queryUuid, diql);

    RResultTable[] res = new RResultTable[1];
    res[0] = null;
    Throwable[] resThrowable = new Throwable[1];
    resThrowable[0] = null;

    UUID executionUuid = queryUuidProvider.createNewExecutionUuid(queryUuid, "master-" + queryUuid);

    QueryExceptionHandler exceptionHandler = new QueryExceptionHandler() {
      @Override
      public void handleException(Throwable t) {
        logger.error("Exception while executing query {} execution {}", queryUuid, executionUuid, t);

        resThrowable[0] = t;

        // shutdown everything.
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid);
      }
    };

    MasterQueryExecutor queryExecutor = new MasterQueryExecutor(executorManager, executionPlanBuilderFactory,
        queryRegistry, new MasterQueryExecutor.QueryExecutorCallback() {
          @Override
          public void intermediaryResultTableAvailable(RResultTable resultTable, short percentDone) {
            // noop
          }

          @Override
          public void finalResultTableAvailable(RResultTable resultTable) {
            logger.trace("Final result for {} execution {}: {}", queryUuid, executionUuid, resultTable);

            res[0] = resultTable;

            // we received the final result, be sure to clean up.
            queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
            executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid);
          }
        }, false);

    queryRegistry.registerQueryExecution(queryUuid, executionUuid, exceptionHandler);

    Runnable execute;
    try {
      execute = queryExecutor.prepareExecution(queryUuid, executionUuid, diql).getLeft();
    } catch (ParseException | ValidationException e) {
      logger.warn("Exception while preparing the query execution of {} execution {}: {}", queryUuid, executionUuid,
          e.getMessage());
      queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
      throw new RQueryException(e.getMessage());
    }

    // start synchronous execution.
    try {
      execute.run();
    } catch (Throwable t) {
      if (resThrowable[0] == null) // take the more specific one if we already found one.
        resThrowable[0] = t;
    }

    if (res[0] != null)
      // if we have a result, we send that.
      return res[0];

    // no result, check if we have a meaningful exception or send a generic error message.
    String exceptionMessage =
        (resThrowable[0] != null) ? resThrowable[0].getMessage() : "Unknown exception while executing query.";
    throw new RQueryException(exceptionMessage);
  }

  /**
   * Executes the given diql query on the diqube cluster, where this node will be the query master.
   * 
   * The query is executed asynchronously and the results will be provided by calling the {@link QueryResultService} at
   * the given {@link RNodeAddress}.
   */
  @Override
  public void asyncExecuteQuery(RUUID queryRUuid, String diql, boolean sendPartialUpdates, RNodeAddress resultAddress)
      throws TException, RQueryException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.info("Async query {}, partial {}, resultAddress {}: {}",
        new Object[] { queryUuid, sendPartialUpdates, resultAddress, diql });

    UUID executionUuid = queryUuidProvider.createNewExecutionUuid(queryUuid, "master-" + queryUuid);

    // will hold the remote execution step, if one is available.
    Holder<ExecuteRemotePlanOnShardsStep> remoteExecutionStepHolder = new Holder<>();
    Holder<ExecutablePlan> masterPlanHolder = new Holder<>();

    SocketListener resultSocketListener = new SocketListener() {
      @Override
      public void connectionDied() {
        // The connection to the "resultAdress" node died.
        logger.error(
            "Connection to result node of query {}, execution {} ({}) died unexpectedly. "
                + "Cancelling execution, there will no results be provided any more.",
            queryUuid, executionUuid, resultAddress);
        // the connection will be returned automatically, the remote node will be removed from ClusterManager
        // automatically.

        if (remoteExecutionStepHolder.getValue() != null)
          cancelExecutionOnTriggeredRemotes(queryRUuid, remoteExecutionStepHolder.getValue());

        queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
        queryRegistry.cleanupQueryFully(queryUuid);
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid);
      }
    };

    Connection<QueryResultService.Client> resultConnection;
    try {
      resultConnection = connectionPool.reserveConnection(QueryResultService.Client.class,
          QueryResultServiceConstants.SERVICE_NAME, resultAddress, resultSocketListener);
    } catch (ConnectionException | InterruptedException e) {
      logger.error("Could not open connection to result node", e);
      throw new RQueryException("Could not open connection to result node: " + e.getMessage());
    }
    QueryResultService.Iface resultService = resultConnection.getService();

    QueryExceptionHandler exceptionHandler = new QueryExceptionHandler() {
      @Override
      public void handleException(Throwable t) {
        logger.error("Exception while executing query " + queryUuid, t);

        try {
          synchronized (resultConnection) {
            resultService.queryException(queryRUuid, new RQueryException(t.getMessage()));
          }
        } catch (TException | RuntimeException e) {
          logger.warn("Was not able to send out exception to " + resultAddress.toString() + " for " + queryUuid, e);
        }

        if (remoteExecutionStepHolder.getValue() != null)
          cancelExecutionOnTriggeredRemotes(queryRUuid, remoteExecutionStepHolder.getValue());

        // shutdown everything.
        connectionPool.releaseConnection(resultConnection);
        queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
        queryRegistry.cleanupQueryFully(queryUuid);
        executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid);
      }
    };

    Deque<QueryStats> remoteStats = new ConcurrentLinkedDeque<>();
    Object remoteStatsWait = new Object();

    queryRegistry.addQueryStatsListener(queryUuid, executionUuid, new QueryRegistry.QueryStatsListener() {
      @Override
      public void queryStatistics(QueryStats stats) {
        remoteStats.add(stats);
        logger.trace("Received remote stats for {}", queryUuid);
        synchronized (remoteStatsWait) {
          remoteStatsWait.notifyAll();
        }
      }
    });

    MasterQueryExecutor queryExecutor = new MasterQueryExecutor(executorManager, executionPlanBuilderFactory,
        queryRegistry, new MasterQueryExecutor.QueryExecutorCallback() {
          @Override
          public void intermediaryResultTableAvailable(RResultTable resultTable, short percentDone) {
            logger.trace("New intermediary result for {}: {}", queryUuid, resultTable);
            try {
              synchronized (resultConnection) {
                // TODO #31 calculate percentages
                resultService.partialUpdate(queryRUuid, resultTable, percentDone);
              }
            } catch (TException e) {
              logger.warn(
                  "Was not able to send out intermediary result to " + resultAddress.toString() + " for " + queryUuid,
                  e);
            }
          }

          @Override
          public void finalResultTableAvailable(RResultTable resultTable) {
            logger.trace("Final result for {}: {}", queryUuid, resultTable);

            try {
              synchronized (resultConnection) {
                resultService.queryResults(queryRUuid, resultTable);
              }
            } catch (TException e) {
              logger.warn("Was not able to send out final result to " + resultAddress.toString() + " for " + queryUuid,
                  e);
            }

            gatherAndSendStatistics();

            // be sure to clean up everything.
            connectionPool.releaseConnection(resultConnection);
            queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
            queryRegistry.cleanupQueryFully(queryUuid);
            executorManager.shutdownEverythingOfQueryExecution(queryUuid, executionUuid); // this will kill our thread,
                                                                                          // too!
          }

          /**
           * Gathers some final stats and tries to send them. Will return cleanly on InterruptedException.
           */
          private void gatherAndSendStatistics() {
            queryRegistry.getOrCreateCurrentStatsManager().setCompletedNanos(System.nanoTime());

            if (masterPlanHolder.getValue() != null && remoteExecutionStepHolder.getValue() != null) {
              ExecutablePlan masterPlan = masterPlanHolder.getValue();
              RExecutionPlan remotePlan = remoteExecutionStepHolder.getValue().getRemoteExecutionPlan();

              new ExecutablePlanQueryStatsUtil().publishQueryStats(queryRegistry.getCurrentStatsManager(), masterPlan);

              // wait some time in case the last remote did not yet provide its statistics.
              int count = 0;
              while (remoteStats.size() != remoteExecutionStepHolder.getValue().getNumberOfRemotesTriggerdOverall()) {
                count++;
                synchronized (remoteStatsWait) {
                  try {
                    remoteStatsWait.wait(100);
                  } catch (InterruptedException e) {
                    return;
                  }
                }

                if (count == 50) { // wait approx. 5s
                  break;
                }
              }

              // only proceed if we now really collected all stats.
              if (remoteStats.size() == remoteExecutionStepHolder.getValue().getNumberOfRemotesTriggerdOverall()) {
                MasterQueryStatisticsMerger statMerger = new MasterQueryStatisticsMerger(masterPlan, remotePlan);

                RQueryStatistics finalStats = statMerger
                    .merge(queryRegistry.getCurrentStatsManager().createQueryStats(), new ArrayList<>(remoteStats));

                logger.trace("Sending out query statistics of {} to client: {}", queryUuid, finalStats);
                try {
                  synchronized (resultConnection) {
                    resultService.queryStatistics(queryRUuid, finalStats);
                  }
                } catch (TException e) {
                  logger.warn(
                      "Was not able to send out query statistics to " + resultAddress.toString() + " for " + queryUuid,
                      e);
                }
              } else
                logger.trace("Not sending statistics for {} as there were not all results received from remotes.",
                    queryUuid);
            }
          }
        }, sendPartialUpdates);

    queryRegistry.registerQueryExecution(queryUuid, executionUuid, exceptionHandler);

    Runnable execute;
    try {
      Triple<Runnable, ExecutablePlan, ExecuteRemotePlanOnShardsStep> t =
          queryExecutor.prepareExecution(queryUuid, executionUuid, diql);
      execute = t.getLeft();
      masterPlanHolder.setValue(t.getMiddle());
      if (t.getRight() != null)
        remoteExecutionStepHolder.setValue(t.getRight());
    } catch (ParseException | ValidationException e) {
      logger.warn("Exception while preparing the query execution of {}: {}", queryUuid, e.getMessage());
      connectionPool.releaseConnection(resultConnection);
      queryRegistry.unregisterQueryExecution(queryUuid, executionUuid);
      queryRegistry.cleanupQueryFully(queryUuid);
      throw new RQueryException(e.getMessage());
    }

    Executor executor =
        executorManager.newQueryFixedThreadPool(1, "query-master-" + queryUuid + "-%d", queryUuid, executionUuid);

    // start asynchronous execution.
    queryRegistry.getOrCreateStatsManager(queryUuid, executionUuid).setStartedNanos(System.nanoTime());
    executor.execute(execute);
  }

  private void cancelExecutionOnTriggeredRemotes(RUUID queryRUuid, ExecuteRemotePlanOnShardsStep remoteStep) {
    // check if we already spawned some calculations on query remotes. If we have any, try to cancel those
    // executions if possible.
    Collection<RNodeAddress> remoteNodesTriggered = remoteStep.getRemotesTriggered();
    if (remoteNodesTriggered != null && !remoteNodesTriggered.isEmpty()) {
      logger.info("Cancelling execution on remotes for query {}: {}", RUuidUtil.toUuid(queryRUuid),
          remoteNodesTriggered);
      for (RNodeAddress triggeredRemote : remoteNodesTriggered) {
        try (Connection<ClusterQueryService.Client> conn = connectionPool.reserveConnection(
            ClusterQueryService.Client.class, ClusterQueryServiceConstants.SERVICE_NAME, triggeredRemote, null)) {
          conn.getService().cancelExecution(queryRUuid);
        } catch (ConnectionException | IOException | TException e) {
          // swallow - if we can't cancel, that's fine, too.
        } catch (InterruptedException e) {
          // end quietly.
          return;
        }
      }
    }
  }

}
