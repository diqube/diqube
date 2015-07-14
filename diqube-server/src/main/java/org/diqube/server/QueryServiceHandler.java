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

import java.util.UUID;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.plan.exception.ParseException;
import org.diqube.plan.exception.ValidationException;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryRegistry.QueryExceptionHandler;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.QueryService.Iface;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RResultTable;
import org.diqube.server.ConnectionPool.Connection;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements logic of a {@link QueryService}.
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

  @Override
  public RResultTable syncExecuteQuery(String diql) throws RQueryException, TException {
    UUID queryUuid = UUID.randomUUID();
    logger.info("Sync query {}: {}", queryUuid, diql);

    RResultTable[] res = new RResultTable[1];
    res[0] = null;
    Throwable[] resThrowable = new Throwable[1];
    resThrowable[0] = null;

    QueryExecutor queryExecutor =
        new QueryExecutor(executorManager, executionPlanBuilderFactory, new QueryExecutor.QueryExecutorCallback() {
          @Override
          public void intermediaryResultTableAvailable(RResultTable resultTable) {
          }

          @Override
          public void finalResultTableAvailable(RResultTable resultTable) {
            logger.trace("Final result for {}: {}", queryUuid, resultTable);

            res[0] = resultTable;

            // we received the final result, be sure to clean up.
            queryRegistry.unregisterQuery(queryUuid);
            executorManager.shutdownEverythingOfQuery(queryUuid);
          }
        }, false);

    Runnable execute;
    try {
      execute = queryExecutor.prepareExecution(queryUuid, diql);
    } catch (ParseException | ValidationException e) {
      logger.warn("Exception while preparing the query execution of {}: {}", queryUuid, e.getMessage());
      throw new RQueryException(e.getMessage());
    }

    // register the query and act correctly in case any of the treads throws an exception during execution.
    queryRegistry.registerQuery(queryUuid, new QueryExceptionHandler() {
      @Override
      public void handleException(Throwable t) {
        logger.error("Exception while executing query " + queryUuid, t);

        resThrowable[0] = t;

        // shutdown everything.
        executorManager.shutdownEverythingOfQuery(queryUuid);
      }
    });

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

  @Override
  public void asyncExecuteQuery(RUUID queryRUuid, String diql, boolean sendPartialUpdates, RNodeAddress resultAddress)
      throws TException, RQueryException {
    UUID queryUuid = RUuidUtil.toUuid(queryRUuid);
    logger.info("Async query {}, partial {}, resultAddress {}: {}",
        new Object[] { queryUuid, sendPartialUpdates, resultAddress, diql });

    Connection<QueryResultService.Client> resultConnection =
        connectionPool.reserveConnection(QueryResultService.Client.class, resultAddress);
    QueryResultService.Iface resultService = resultConnection.getService();

    QueryExecutor queryExecutor =
        new QueryExecutor(executorManager, executionPlanBuilderFactory, new QueryExecutor.QueryExecutorCallback() {
          @Override
          public void intermediaryResultTableAvailable(RResultTable resultTable) {
            logger.trace("New intermediary result for {}: {}", queryUuid, resultTable);
            try {
              synchronized (resultConnection) {
                // TODO #31 calculate percentages
                resultService.partialUpdate(queryRUuid, resultTable, (short) 1);
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

            // we received the final result, be sure to clean up.
            connectionPool.releaseConnection(resultConnection);
            queryRegistry.unregisterQuery(queryUuid);
            executorManager.shutdownEverythingOfQuery(queryUuid); // this will kill our thread, too!
          }
        }, sendPartialUpdates);

    Runnable execute;
    try {
      execute = queryExecutor.prepareExecution(queryUuid, diql);
    } catch (ParseException | ValidationException e) {
      logger.warn("Exception while preparing the query execution of {}: {}", queryUuid, e.getMessage());
      throw new RQueryException(e.getMessage());
    }

    // register the query and act correctly in case any of the treads throws an exception during execution.
    queryRegistry.registerQuery(queryUuid, new QueryExceptionHandler() {
      @Override
      public void handleException(Throwable t) {
        logger.error("Exception while executing query " + queryUuid, t);

        try {
          synchronized (resultConnection) {
            resultService.queryException(queryRUuid, new RQueryException(t.getMessage()));
          }
        } catch (TException e) {
          logger.warn("Was not able to send out exception to " + resultAddress.toString() + " for " + queryUuid, e);
        }

        // shutdown everything.
        connectionPool.releaseConnection(resultConnection);
        executorManager.shutdownEverythingOfQuery(queryUuid);
      }
    });

    Executor executor = executorManager.newQueryFixedThreadPool(1, "query-master-" + queryUuid + "-%d", queryUuid);

    // start asynchronous execution.
    executor.execute(execute);
  }

}
