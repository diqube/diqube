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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
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

  private Set<UUID> toCancelQueries = new ConcurrentSkipListSet<>();

  /**
   * Cancels the execution of a query that was started with
   * {@link #asyncExecuteQuery(RUUID, String, boolean, RNodeAddress)}.
   * 
   * Note that this method must be called on the same node on which the
   * {@link #asyncExecuteQuery(RUUID, String, boolean, RNodeAddress)} was called!
   */
  @Override
  public void cancelQueryExecution(RUUID queryRUuid) throws TException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.info("Received request to cancel query {}. Cancelling.", queryUuid);

    toCancelQueries.add(queryUuid);

    // We only need to cancel the query master locally - and we do this by pretending that there was an exception. The
    // exception handler (defined inside the #asyncExecuteQuery method below) will then cancel execution on all remotes
    // and will shutdown everything on this node as well.
    UUID queryMasterExecutionUuid = queryRegistry.getMasterExecutionUuid(queryUuid);
    if (queryMasterExecutionUuid == null)
      // That query is still inside the #asyncExecuteQuery method and it did not start executing yet. We'll cancel that
      // query right inside the #asyncExecuteQuery method.
      return;

    queryRegistry.handleException(queryUuid, queryMasterExecutionUuid, new RuntimeException("Cancelled by user"));

    toCancelQueries.remove(queryUuid);
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

        queryRegistry.cleanupQueryFully(queryUuid);
        // kill all executions, also remote ones.
        executorManager.shutdownEverythingOfQuery(queryUuid);
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
            logger.trace("Sent exception to client, query {}", queryUuid);
          }
        } catch (TException | RuntimeException e) {
          logger.warn("Was not able to send out exception to " + resultAddress.toString() + " for " + queryUuid, e);
        }

        if (remoteExecutionStepHolder.getValue() != null)
          cancelExecutionOnTriggeredRemotes(queryRUuid, remoteExecutionStepHolder.getValue());
        else
          logger.trace("Cannot cancel execution of {} on remotes because I do not know about the remotes", queryUuid);

        // shutdown everything.
        connectionPool.releaseConnection(resultConnection);
        queryRegistry.cleanupQueryFully(queryUuid);
        // kill all executions, also remote ones.
        executorManager.shutdownEverythingOfQuery(queryUuid);
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
            queryRegistry.cleanupQueryFully(queryUuid);
            // kill all executions, also remote ones. THis will kill our thread, too.
            executorManager.shutdownEverythingOfQuery(queryUuid);
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

          @Override
          public void exception(Throwable t) {
            logger.trace("Exception when executing {}: {}", queryUuid, t);

            try {
              synchronized (resultConnection) {
                resultService.queryException(queryRUuid, new RQueryException(t.getMessage()));
              }
            } catch (TException e) {
              logger.warn(
                  "Was not able to send out exception result to " + resultAddress.toString() + " for " + queryUuid, e);
            }

            if (remoteExecutionStepHolder.getValue() != null)
              cancelExecutionOnTriggeredRemotes(queryRUuid, remoteExecutionStepHolder.getValue());

            // be sure to clean up everything.
            connectionPool.releaseConnection(resultConnection);
            queryRegistry.cleanupQueryFully(queryUuid);
            // kill all executions, also remote ones. This will kill our thread, too.
            executorManager.shutdownEverythingOfQuery(queryUuid);
          }
        }, sendPartialUpdates);

    queryRegistry.registerQueryExecution(queryUuid, executionUuid, exceptionHandler, true);

    if (toCancelQueries.contains(queryUuid)) {
      // query was cancelled already - as we did not yet start executing it, we can simply return here.
      resultService.queryException(queryRUuid, new RQueryException("Query cancelled"));
      connectionPool.releaseConnection(resultConnection);
      queryRegistry.cleanupQueryFully(queryUuid);
      toCancelQueries.remove(queryUuid);
      return;
    }

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
      queryRegistry.cleanupQueryFully(queryUuid);
      throw new RQueryException(e.getMessage());
    }

    Executor executor = executorManager.newQueryFixedThreadPoolWithTimeout(1, "query-master-" + queryUuid + "-%d",
        queryUuid, executionUuid);

    // start asynchronous execution.
    queryRegistry.getOrCreateStatsManager(queryUuid, executionUuid).setStartedNanos(System.nanoTime());
    executor.execute(execute);
  }

  private void cancelExecutionOnTriggeredRemotes(RUUID queryRUuid, ExecuteRemotePlanOnShardsStep remoteStep) {
    // check if we already spawned some calculations on query remotes. If we have any, try to cancel those
    // executions if possible.
    Collection<RNodeAddress> remoteNodesActive = remoteStep.getRemotesActive();
    if (remoteNodesActive != null && !remoteNodesActive.isEmpty()) {
      logger.info("Cancelling execution on remotes for query {}: {}", RUuidUtil.toUuid(queryRUuid), remoteNodesActive);
      for (RNodeAddress triggeredRemote : remoteNodesActive) {
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
    } else
      logger.trace("Cannot cancel execution on remotes. remoteNodesActive: {}", remoteNodesActive);
  }

}
