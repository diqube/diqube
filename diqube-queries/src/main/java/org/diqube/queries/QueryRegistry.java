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
package org.diqube.queries;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.diqube.context.AutoInstatiate;
import org.diqube.function.IntermediaryResult;
import org.diqube.util.Pair;

/**
 * All queries being executed are registered here. This class will be informed about any asynchronous exceptions that
 * are thrown inside the {@link ExecutorService}s of a query and will inform a specific {@link QueryExceptionHandler}
 * that can be installed for each query.
 * 
 * <p>
 * All queries that will be executed should be (1) registered in this class and (2) all {@link Executor}s that will
 * execute anything for the query should be created by the ExecutorManager (diqube-threads).
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryRegistry {
  private Map<Pair<UUID, UUID>, QueryExceptionHandler> exceptionHandlers = new ConcurrentHashMap<>();
  private Map<UUID, Deque<QueryResultHandler>> resultHandlers = new ConcurrentHashMap<>();

  /**
   * Register a query, its execution and its exception handler. Note that for the query
   * {@link #unregisterQueryExecution(UUID)} has to be called. For query UUID/execution UUID, see {@link QueryUuid} and
   * ExecutablePlan.
   */
  public void registerQueryExecution(UUID queryUuid, UUID executionUuid, QueryExceptionHandler exceptionHandler) {
    exceptionHandlers.put(new Pair<>(queryUuid, executionUuid), exceptionHandler);
  }

  /**
   * Add a {@link QueryResultHandler} that will receive results of query remotes from a query execution. Please note
   * that the resultHandler needs to be {@link #removeQueryResultHandler(UUID, UUID, QueryResultHandler) unregistered}
   * again. This method only makes sense to be called on the query master.
   */
  public void addQueryResultHandler(UUID queryUuid, QueryResultHandler resultHandler) {
    if (!resultHandlers.containsKey(queryUuid)) {
      synchronized (queryUuid) {
        if (!resultHandlers.containsKey(queryUuid)) {
          resultHandlers.put(queryUuid, new ConcurrentLinkedDeque<>());
        }
      }
    }

    resultHandlers.get(queryUuid).add(resultHandler);
  }

  /**
   * Remove a specific resultHandler.
   */
  public void removeQueryResultHandler(UUID queryUuid, QueryResultHandler resultHandler) {
    Deque<QueryResultHandler> deque = resultHandlers.get(queryUuid);
    if (deque == null)
      return;

    while (deque.remove(resultHandler))
      ;

    if (deque.isEmpty()) {
      synchronized (queryUuid) {
        if (deque.isEmpty())
          resultHandlers.remove(queryUuid);
      }
    }
  }

  /**
   * Get and return all {@link QueryResultHandler}s registered for a specific query.
   * 
   * @return the result handlers. Could be empty.
   */
  public Collection<QueryResultHandler> getQueryResultHandlers(UUID queryUuid) {
    Deque<QueryResultHandler> deque = resultHandlers.get(queryUuid);
    if (deque == null)
      return new ArrayList<>();

    return new ArrayList<>(deque);
  }

  /**
   * Unregisters a query execution, all exceptions that will be thrown in one of the corresponding {@link Executor}s in
   * the future will not be passed on to the registered {@link QueryExceptionHandler} any more!
   */
  public void unregisterQueryExecution(UUID queryUuid, UUID executionUuid) {
    exceptionHandlers.remove(new Pair<>(queryUuid, executionUuid));
  }

  /**
   * Call if an exception occurred while executing a given query.
   * 
   * @return <code>true</code> when the exception was handled by an exception handler, <code>false</code> otherwise.
   */
  public boolean handleException(UUID queryUuid, UUID executionUuid, Throwable t) {
    QueryExceptionHandler exceptionHandler = exceptionHandlers.get(new Pair<>(queryUuid, executionUuid));
    if (exceptionHandler == null)
      return false;

    exceptionHandler.handleException(t);
    unregisterQueryExecution(queryUuid, executionUuid);
    return true;
  }

  /**
   * Handle and exception that occurred in a thread while executing a query.
   */
  public static interface QueryExceptionHandler {
    /**
     * Handle the given exception. The query execution will automatically be unregistered in the {@link QueryRegistry}.
     */
    public void handleException(Throwable t);
  }

  /**
   * Handles results that are received by the ClusterQueryService from query remotes.
   * 
   * This needs to be implemented on the query master.
   */
  public static interface QueryResultHandler {
    /**
     * A new {@link IntermediaryResult} is available from a remote.
     */
    public void newIntermediaryAggregationResult(long groupId, String colName,
        IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
        IntermediaryResult<Object, Object, Object> newIntermediaryResult);

    /**
     * New column values are available from a remote.
     */
    public void newColumnValues(String colName, Map<Long, Object> values);

    /**
     * One remote reported that it is done processing the request.
     */
    public void oneRemoteDone();

    /**
     * One remote reported that there was an exception processing the request. That one remote stopped processing
     * therefore.
     */
    public void oneRemoteException(String msg);
  }
}
