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
package org.diqube.threads;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages {@link ExecutorService}s for diqube.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ExecutorManager {
  private static final Logger logger = LoggerFactory.getLogger(ExecutorManager.class);

  private Map<UUID, List<DiqubeFixedThreadPoolExecutor>> queryExecutors = new HashMap<>();

  @Inject
  private QueryRegistry queryRegistry;

  /**
   * Create a new cached thread pool, see {@link Executors#newCachedThreadPool()}.
   * 
   * @param nameFormat
   *          a {@link String#format(String, Object...)}-compatible format String, to which a unique integer (0, 1,
   *          etc.) will be supplied as the single parameter. This integer will be unique to the built instance of the
   *          ThreadFactory and will be assigned sequentially. For example, {@code "rpc-pool-%d"} will generate thread
   *          names like {@code "rpc-pool-0"}, {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
   * @param uncaughtExceptionHandler
   *          This will be called in case any of the threads of the ExecutorService ends because an exception was
   *          thrown.
   * 
   * @return The new cached thread pool.
   */
  public ExecutorService newCachedThreadPool(String nameFormat, UncaughtExceptionHandler uncaughtExceptionHandler) {
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
    threadFactoryBuilder.setNameFormat(nameFormat);
    threadFactoryBuilder.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    return Executors.newCachedThreadPool(threadFactoryBuilder.build());
  }

  /**
   * Create a new thread pool with a fixed set of threads, see {@link Executors#newFixedThreadPool(int)}. The returned
   * {@link ExecutorService} should be used for executing a specific diql query.
   * 
   * <p>
   * All threads that are used by the returned {@link Executor} will be "bound" to the specific diql query: That means
   * that in case there is an uncaught exception thrown by one of those threads, the {@link QueryRegistry} will be
   * informed about this and a potentially installed exception handler for that query will be called.
   * 
   * <p>
   * In addition to that, the returned {@link Executor} will also be affected when any of the following methods are
   * called with the specific query ID:
   * 
   * <ul>
   * <li>{@link #findQueryUuidOfExecutorService(ExecutorService)}
   * <li>{@link #findAllExecutorServicesOfQuery(UUID)}
   * <li>{@link #shutdownEverythingOfQuery(UUID)}
   * <li>{@link #shutdownEverythingOfAllQueries()}
   * </ul>
   * 
   * TODO #30 implement a timeout on the returned Executor, so there are no executors that will run forever.
   * 
   * @param numberOfThreads
   *          Number of threads the thread pool should have
   * @param nameFormat
   *          a {@link String#format(String, Object...)}-compatible format String, to which a unique integer (0, 1,
   *          etc.) will be supplied as the single parameter. This integer will be unique to the built instance of the
   *          ThreadFactory and will be assigned sequentially. For example, {@code "rpc-pool-%d"} will generate thread
   *          names like {@code "rpc-pool-0"}, {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
   * @param queryUuid
   *          The UUID to whose execution the returned ExecutorService belongs to.
   * 
   * @return The new thread pool. It is not a {@link ExecutorService}, but only an {@link Executor} returned, because
   *         ONLY the {@link Executor#execute(Runnable)} method must be run, because then the exception forwarding which
   *         is described above will work correctly. This does not work when the method
   *         {@link ExecutorService#submit(java.util.concurrent.Callable)} etc. are called, becuase the ExecutorService
   *         won't forward the exception in that case, but encapsulate it in the corresponding {@link Future}.
   */
  public synchronized Executor newQueryFixedThreadPool(int numberOfThreads, String nameFormat, UUID queryUuid) {
    ThreadFactoryBuilder baseThreadFactoryBuilder = new ThreadFactoryBuilder();
    baseThreadFactoryBuilder.setNameFormat(nameFormat);
    // Use our ThreadFactory as facette in order to install our exception handling and enable the publication of the
    // query UUID in QueryUuid when any thread of the query starts running.
    ThreadFactory threadFactory = new QueryThreadFactory(baseThreadFactoryBuilder.build(), queryUuid, queryRegistry);

    DiqubeFixedThreadPoolExecutor res = new DiqubeFixedThreadPoolExecutor(numberOfThreads, threadFactory, queryUuid);
    res.setThreadNameFormatForToString(nameFormat);
    synchronized (queryExecutors) {
      if (!queryExecutors.containsKey(queryUuid))
        queryExecutors.put(queryUuid, new ArrayList<>());
      queryExecutors.get(queryUuid).add(res);
    }

    return res;
  }

  /**
   * @return The Query UUID of the given {@link Executor} that was created by
   *         {@link #newQueryFixedThreadPool(int, String, UUID)}. <code>null</code> if not available.
   */
  public UUID findQueryUuidOfExecutorService(ExecutorService service) {
    if (!(service instanceof DiqubeFixedThreadPoolExecutor))
      return null;
    return ((DiqubeFixedThreadPoolExecutor) service).getQueryUuid();
  }

  /**
   * @return All {@link Executor}s that are registered as executing some work for the given query.
   */
  public List<DiqubeFixedThreadPoolExecutor> findAllExecutorServicesOfQuery(UUID queryUuid) {
    synchronized (queryExecutors) {
      return new ArrayList<>(queryExecutors.get(queryUuid));
    }
  }

  /**
   * Calls {@link ExecutorService#shutdownNow()} on all Executors that are registered for the given query and
   * unregisters those executors. The executors have to have been created using
   * {@link #newQueryFixedThreadPool(int, String, UUID)}.
   */
  public synchronized void shutdownEverythingOfQuery(UUID queryUuid) {
    List<DiqubeFixedThreadPoolExecutor> executors = findAllExecutorServicesOfQuery(queryUuid);
    if (executors != null) {
      logger.trace("Shutting down {} executors of query {}: {}", executors.size(), queryUuid, executors);
      for (DiqubeFixedThreadPoolExecutor executor : executors)
        executor.shutdownNow();
    }
    synchronized (queryExecutors) {
      queryExecutors.remove(queryUuid);
    }
  }

  /**
   * Calls {@link ExecutorService#shutdownNow()} on all Executors that have been created using
   * {@link #newQueryFixedThreadPool(int, String, UUID)} and which are still active.
   */
  public synchronized void shutdownEverythingOfAllQueries() {
    synchronized (queryExecutors) {
      for (Iterator<Entry<UUID, List<DiqubeFixedThreadPoolExecutor>>> it = queryExecutors.entrySet().iterator(); it
          .hasNext();) {
        Entry<UUID, List<DiqubeFixedThreadPoolExecutor>> e = it.next();
        for (DiqubeFixedThreadPoolExecutor executor : e.getValue())
          executor.shutdownNow();
        it.remove();
      }
    }
  }
}
