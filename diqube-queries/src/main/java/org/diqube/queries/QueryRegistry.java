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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.function.IntermediaryResult;
import org.diqube.listeners.providers.OurNodeAddressProvider;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All queries being executed are registered here. This class will be informed about any asynchronous exceptions that
 * are thrown inside the {@link ExecutorService}s of a query and will inform a specific {@link QueryExceptionHandler}
 * that can be installed for each query.
 * 
 * <p>
 * All queries that will be executed should be (1) registered in this class and (2) all {@link Executor}s that will
 * execute anything for the query should be created by the ExecutorManager (diqube-threads), to make sure that these
 * threads have a valid {@link QueryUuidThreadState} set.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class QueryRegistry {
  private static final Logger logger = LoggerFactory.getLogger(QueryRegistry.class);

  /**
   * Map from queryUuid/executionUuid pair to Pair of exception handler for that execution and a boolean indicating if
   * the queryUuid/executionUuid pair is the pair for the query master of that queryUuid.
   */
  private ConcurrentNavigableMap<Pair<UUID, UUID>, Pair<QueryExceptionHandler, Boolean>> queryExecutionInformation =
      new ConcurrentSkipListMap<>();
  private ConcurrentMap<UUID, Deque<QueryResultHandler>> resultHandlers = new ConcurrentHashMap<>();
  private ConcurrentMap<UUID, Deque<QueryPercentHandler>> percentHandlers = new ConcurrentHashMap<>();
  private ConcurrentMap<UUID, QueryStatsManager> queryStats = new ConcurrentHashMap<>();
  private ConcurrentMap<UUID, Map<UUID, QueryStatsListener>> queryStatsListeners = new ConcurrentHashMap<>();

  @Config(ConfigKey.OUR_HOST)
  private String ourHost;

  @Config(ConfigKey.PORT)
  private int ourPort;

  @InjectOptional
  private OurNodeAddressProvider ourNodeAddressProvider;

  /**
   * Register a query, its execution and its exception handler. Note that for the query
   * {@link #unregisterQueryExecution(UUID)} has to be called. For query UUID/execution UUID, see {@link QueryUuid} and
   * ExecutablePlan.
   */
  public void registerQueryExecution(UUID queryUuid, UUID executionUuid, QueryExceptionHandler exceptionHandler,
      boolean isQueryMaster) {
    queryExecutionInformation.put(new Pair<>(queryUuid, executionUuid), new Pair<>(exceptionHandler, isQueryMaster));
  }

  /**
   * Add a handler for percentages that were reported by remotes.
   * 
   * Be sure to call {@link #removeRemotePercentHanlder(UUID, QueryPercentHandler)}.
   */
  public void addRemotePercentHandler(UUID queryUuid, QueryPercentHandler percentHandler) {
    synchronized (percentHandlers) {
      if (!percentHandlers.containsKey(queryUuid))
        percentHandlers.put(queryUuid, new ConcurrentLinkedDeque<>());
      percentHandlers.get(queryUuid).add(percentHandler);
    }
  }

  /**
   * Removes a {@link QueryPercentHandler} and releases any reserved resources.
   */
  public void removeRemotePercentHanlder(UUID queryUuid, QueryPercentHandler percentHandler) {
    if (percentHandlers.containsKey(queryUuid)) {
      synchronized (percentHandlers) {
        if (percentHandlers.containsKey(queryUuid)) {
          percentHandlers.get(queryUuid).remove(percentHandler);
          if (percentHandlers.get(queryUuid).isEmpty())
            percentHandlers.remove(queryUuid);
        }
      }
    }
  }

  /**
   * Get the registered {@link QueryPercentHandler}s for a query or return an empty list.
   */
  public List<QueryPercentHandler> getQueryPercentHandlers(UUID queryUuid) {
    Deque<QueryPercentHandler> handlers = percentHandlers.get(queryUuid);
    if (handlers == null)
      return new ArrayList<>();
    return new ArrayList<>(handlers);
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
    logger.trace("Unregistering query {} execution {}", queryUuid, executionUuid);
    queryExecutionInformation.remove(new Pair<>(queryUuid, executionUuid));
    queryStats.remove(executionUuid);
    if (queryStatsListeners.containsKey(queryUuid)) {
      synchronized (queryStatsListeners) {
        if (queryStatsListeners.containsKey(queryUuid)) {
          queryStatsListeners.get(queryUuid).remove(executionUuid);
          if (queryStatsListeners.get(queryUuid).isEmpty())
            queryStatsListeners.remove(queryUuid);
        }
      }
    }
  }

  /**
   * Cleans up any resources that might be remaining for the given query. THis should only be called on the query
   * master: There might be a query remote being executed on the same node as the master and that node must not clear
   * any resources of the master!
   */
  public void cleanupQueryFully(UUID queryUuid) {
    logger.trace("Cleaning up query {} fully.", queryUuid);

    // pair of queryUuid to executionUuid for the given query.
    Set<Pair<UUID, UUID>> queryExecutionPairs = new HashSet<>();

    // exceptionHandlers is sorted. We pick a sub-map that will start with the Pair<UUID, UUID>s that contain our
    // queryUuid. As soon as we find another UUID as "left" we can stop traversing, because there will be no other pair
    // with our queryUuid.
    NavigableMap<Pair<UUID, UUID>, Pair<QueryExceptionHandler, Boolean>> searchMap =
        queryExecutionInformation.tailMap(new Pair<>(queryUuid, new UUID(Long.MIN_VALUE, Long.MIN_VALUE)));
    for (Pair<UUID, UUID> p : searchMap.keySet()) {
      if (!p.getLeft().equals(queryUuid))
        break;
      queryExecutionPairs.add(p);
    }

    for (Pair<UUID, UUID> p : queryExecutionPairs)
      queryExecutionInformation.remove(p);

    Set<UUID> executionUuids = queryExecutionPairs.stream().map(p -> p.getRight()).collect(Collectors.toSet());

    synchronized (percentHandlers) {
      percentHandlers.remove(queryUuid);
    }
    synchronized (queryStats) {
      for (UUID executionUuid : executionUuids)
        queryStats.remove(executionUuid);
    }
    synchronized (queryUuid) {
      resultHandlers.remove(queryUuid);
    }
    synchronized (queryStatsListeners) {
      queryStatsListeners.remove(queryUuid);
    }
  }

  /**
   * Tries to find the executionUuid of the query master of the given query.
   * 
   * If it is not available locally, <code>null</code> will be returned.
   */
  public UUID getMasterExecutionUuid(UUID queryUuid) {
    Pair<UUID, UUID> masterPair = null;
    NavigableMap<Pair<UUID, UUID>, Pair<QueryExceptionHandler, Boolean>> searchMap =
        queryExecutionInformation.tailMap(new Pair<>(queryUuid, new UUID(Long.MIN_VALUE, Long.MIN_VALUE)));
    for (Entry<Pair<UUID, UUID>, Pair<QueryExceptionHandler, Boolean>> e : searchMap.entrySet()) {

      if (!e.getKey().getLeft().equals(queryUuid))
        break;

      if (e.getValue().getRight()) {
        masterPair = e.getKey();
        break;
      }
    }

    if (masterPair == null)
      return null;

    return masterPair.getRight();
  }

  /**
   * Call if an exception occurred while executing a given query.
   * 
   * @return <code>true</code> when the exception was handled by an exception handler, <code>false</code> otherwise.
   */
  public boolean handleException(UUID queryUuid, UUID executionUuid, Throwable t) {
    Pair<QueryExceptionHandler, Boolean> p = queryExecutionInformation.get(new Pair<>(queryUuid, executionUuid));
    if (p == null)
      return false;

    p.getLeft().handleException(t);
    unregisterQueryExecution(queryUuid, executionUuid);
    return true;
  }

  /**
   * @return The currently active {@link QueryStats}, there is one created if not yet available.
   * @throws IllegalStateException
   *           If current queryUuid or executionUuid cannot be found.
   */
  public QueryStatsManager getOrCreateCurrentStatsManager() throws IllegalStateException {
    UUID queryUuid = QueryUuid.getCurrentQueryUuid();
    UUID executionUuid = QueryUuid.getCurrentExecutionUuid();
    if (queryUuid == null || executionUuid == null)
      throw new IllegalStateException("No current query and execution!");
    return getOrCreateStatsManager(queryUuid, executionUuid);
  }

  /**
   * Gets, but does not create the current statistics
   * 
   * @return the current {@link QueryStats} or <code>null</code>.
   * @throws IllegalStateException
   *           If current executionUuid cannot be determined.
   */
  public QueryStatsManager getCurrentStatsManager() throws IllegalStateException {
    UUID executionUuid = QueryUuid.getCurrentExecutionUuid();
    if (executionUuid == null)
      throw new IllegalStateException("No current query and execution!");
    return queryStats.get(executionUuid);
  }

  /**
   * Get or create a QueryStats object for the given query/execution.
   * 
   * @return The active {@link QueryStats} for that query/execution, there is one created if not yet available.
   */
  public QueryStatsManager getOrCreateStatsManager(UUID queryUuid, UUID executionUuid) {
    if (!queryStats.containsKey(executionUuid)) {
      synchronized (queryStats) {
        if (!queryStats.containsKey(executionUuid)) {
          String ourAddr;
          if (ourNodeAddressProvider != null)
            ourAddr = ourNodeAddressProvider.getOurNodeAddress();
          else
            ourAddr = ourHost + ":" + ourPort;

          queryStats.put(executionUuid, new QueryStatsManager(ourAddr));
        }
      }
    }

    return queryStats.get(executionUuid);
  }

  /**
   * Add a listener which gets informed when query remotes inform about their query statistics on the given query UUID.
   */
  public void addQueryStatsListener(UUID queryUuid, UUID executionUuid, QueryStatsListener listener) {
    if (!queryStatsListeners.containsKey(queryUuid)) {
      synchronized (queryStatsListeners) {
        if (!queryStatsListeners.containsKey(queryUuid))
          queryStatsListeners.putIfAbsent(queryUuid, new ConcurrentHashMap<>());
      }
    }

    queryStatsListeners.get(queryUuid).put(executionUuid, listener);
  }

  /**
   * As soon as a remote reported query statistics, call this method in order to inform everybody who is interested.
   */
  public void remoteQueryStatsAvailable(UUID queryUuid, QueryStats stats) {
    Map<UUID, QueryStatsListener> listeners = queryStatsListeners.getOrDefault(queryUuid, new HashMap<>());
    listeners.values().forEach(listener -> listener.queryStatistics(stats));
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
     * 
     * A single remote will only call this method after all other calls of that remote to other methods of this
     * interface have returned.
     */
    public void oneRemoteDone();

    /**
     * One remote reported that there was an exception processing the request. That one remote stopped processing
     * therefore.
     */
    public void oneRemoteException(String msg);
  }

  /**
   * Listener that is called when new statistics have been reported by query remotes.
   */
  public static interface QueryStatsListener {
    public void queryStatistics(QueryStats stats);
  }

  /**
   * Listener for data about how much of an executablePlan was calculated by remotes already.
   */
  public static interface QueryPercentHandler {
    /**
     * A single remote reported a new percent-delta.
     */
    public void newRemoteCompletionPercentDelta(short percentDeltaOfSingleRemote);
  }
}
