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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
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

  /**
   * Map from query UUID to execution UUID to list of executors registered for it. Usually there should be one Executor
   * for a queryUuid/executorUuid combination.
   */
  private Map<UUID, Map<UUID, List<DiqubeFixedThreadPoolExecutor>>> queryExecutors = new HashMap<>();

  private ShutdownThread shutdownThread = new ShutdownThread();

  @Inject
  private QueryRegistry queryRegistry;

  @PostConstruct
  public void initialize() {
    shutdownThread.start();
  }

  @PreDestroy
  public void cleanup() {
    shutdownThread.interrupt();
  }

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
   * Create a new {@link ExecutorService} that does create threads as needed, but contains a maxmimum number of threads.
   * 
   * @param nameFormat
   *          a {@link String#format(String, Object...)}-compatible format String, to which a unique integer (0, 1,
   *          etc.) will be supplied as the single parameter. This integer will be unique to the built instance of the
   *          ThreadFactory and will be assigned sequentially. For example, {@code "rpc-pool-%d"} will generate thread
   *          names like {@code "rpc-pool-0"}, {@code "rpc-pool-1"}, {@code "rpc-pool-2"}, etc.
   * @param uncaughtExceptionHandler
   *          This will be called in case any of the threads of the ExecutorService ends because an exception was
   *          thrown.
   * @param maxPoolSize
   *          Maximum number of threads.
   * @return The new {@link ExecutorService}.
   */
  public ExecutorService newCachedThreadPoolWithMax(String nameFormat,
      UncaughtExceptionHandler uncaughtExceptionHandler, int maxPoolSize) {
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
    threadFactoryBuilder.setNameFormat(nameFormat);
    threadFactoryBuilder.setUncaughtExceptionHandler(uncaughtExceptionHandler);

    return new ThreadPoolExecutor(0, maxPoolSize, 10, TimeUnit.SECONDS, new SynchronousQueue<>(),
        threadFactoryBuilder.build());
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
   * <li>{@link #findAllExecutorServicesOfQueryExecution(UUID)}
   * <li>{@link #shutdownEverythingOfQueryExecution(UUID)}
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
   *          The UUID to whose execution the returned {@link Executor} belongs to. For a description of query
   *          UUID/executor UUID, see {@link QueryUuid} and ExecutablePlan.
   * @param executionUuid
   *          The UUID of the execution the returned {@link Executor} belongs to. For a description of query
   *          UUID/executor UUID, see {@link QueryUuid} and ExecutablePlan.
   * 
   * @return The new thread pool. It is not a {@link ExecutorService}, but only an {@link Executor} returned, because
   *         ONLY the {@link Executor#execute(Runnable)} method must be run, because then the exception forwarding which
   *         is described above will work correctly. This does not work when the method
   *         {@link ExecutorService#submit(java.util.concurrent.Callable)} etc. are called, becuase the ExecutorService
   *         won't forward the exception in that case, but encapsulate it in the corresponding {@link Future}.
   */
  public synchronized Executor newQueryFixedThreadPool(int numberOfThreads, String nameFormat, UUID queryUuid,
      UUID executionUuid) {
    ThreadFactoryBuilder baseThreadFactoryBuilder = new ThreadFactoryBuilder();
    baseThreadFactoryBuilder.setNameFormat(nameFormat);
    // Use our ThreadFactory as facette in order to install our exception handling and enable the publication of the
    // query & execution UUID in QueryUuid when any thread of the query starts running.
    ThreadFactory threadFactory =
        new QueryThreadFactory(baseThreadFactoryBuilder.build(), queryUuid, executionUuid, queryRegistry);

    DiqubeFixedThreadPoolExecutor res = new DiqubeFixedThreadPoolExecutor(numberOfThreads, threadFactory, queryUuid);
    res.setThreadNameFormatForToString(nameFormat);
    synchronized (queryExecutors) {
      if (!queryExecutors.containsKey(queryUuid))
        queryExecutors.put(queryUuid, new HashMap<>());
      if (!queryExecutors.get(queryUuid).containsKey(executionUuid))
        queryExecutors.get(queryUuid).put(executionUuid, new ArrayList<>());
      queryExecutors.get(queryUuid).get(executionUuid).add(res);
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
  public List<DiqubeFixedThreadPoolExecutor> findAllExecutorServicesOfQueryExecution(UUID queryUuid,
      UUID executionUuid) {
    synchronized (queryExecutors) {
      if (!queryExecutors.containsKey(queryUuid) || !queryExecutors.get(queryUuid).containsKey(executionUuid))
        return null;
      return new ArrayList<>(queryExecutors.get(queryUuid).get(executionUuid));
    }
  }

  /**
   * Calls {@link ExecutorService#shutdownNow()} on all Executors that are registered for the given query execution and
   * unregisters those executors. The executors have to have been created using
   * {@link #newQueryFixedThreadPool(int, String, UUID)}.
   */
  public synchronized void shutdownEverythingOfQueryExecution(UUID queryUuid, UUID executionUuid) {
    Collection<ExecutorService> shutdownExecutors = new ArrayList<>();
    List<DiqubeFixedThreadPoolExecutor> executors = findAllExecutorServicesOfQueryExecution(queryUuid, executionUuid);
    if (executors != null) {
      logger.trace("Shutting down {} executors of query {} execution {}: {}", executors.size(), queryUuid,
          executionUuid, executors);
      for (DiqubeFixedThreadPoolExecutor executor : executors)
        shutdownExecutors.add(executor);
    }
    synchronized (queryExecutors) {
      queryExecutors.get(queryUuid).remove(executionUuid);

      if (queryExecutors.get(queryUuid).isEmpty())
        queryExecutors.remove(queryUuid);
    }

    synchronized (shutdownThread.shutdownExecutors) {
      shutdownThread.shutdownExecutors.addAll(shutdownExecutors);
      shutdownThread.numberOfServicesToShutdown.addAndGet(shutdownExecutors.size());
      synchronized (shutdownThread.sync) {
        shutdownThread.sync.notifyAll();
      }
    }
  }

  /**
   * Calls {@link ExecutorService#shutdownNow()} on all Executors that have been created using
   * {@link #newQueryFixedThreadPool(int, String, UUID)} and which are still active.
   */
  public synchronized void shutdownEverythingOfAllQueries() {
    Collection<ExecutorService> shutdownExecutors = new ArrayList<>();
    synchronized (queryExecutors) {
      for (Iterator<Entry<UUID, Map<UUID, List<DiqubeFixedThreadPoolExecutor>>>> it =
          queryExecutors.entrySet().iterator(); it.hasNext();) {
        Entry<UUID, Map<UUID, List<DiqubeFixedThreadPoolExecutor>>> e = it.next();
        for (Entry<UUID, List<DiqubeFixedThreadPoolExecutor>> e2 : e.getValue().entrySet())
          for (DiqubeFixedThreadPoolExecutor executor : e2.getValue())
            shutdownExecutors.add(executor);
        it.remove();
      }
    }

    synchronized (shutdownThread.shutdownExecutors) {
      shutdownThread.shutdownExecutors.addAll(shutdownExecutors);
      shutdownThread.numberOfServicesToShutdown.addAndGet(shutdownExecutors.size());
      synchronized (shutdownThread.sync) {
        shutdownThread.sync.notifyAll();
      }
    }
  }

  /**
   * Thread that is used to shut down any created {@link Executor}s.
   * 
   * This needs to be done in a separate thread, in case the
   * {@link ExecutorManager#shutdownEverythingOfQueryExecution(UUID, Collection)} or
   * {@link ExecutorManager#shutdownEverythingOfAllQueries()} is called within a thread that belongs to one of the
   * Executors that will be shutdown. In that case we might end up not shutting down some, because the thread executing
   * the shutdowns was killed before.
   * 
   * <p>
   * Communicating with this thread:
   * <ul>
   * <li>Start sync on {@link #shutdownExecutors}.
   * <li>Add the executors that should be shutdown to the end of {@link #shutdownExecutors}.
   * <li>Increase {@link #numberOfServicesToShutdown} by the number of executors you added to {@link #shutdownExecutors}
   * <li>Start sync on {@link #sync}
   * <li>call {@link #sync#notifyAll()}.
   * <li>Leave sync on {@link #sync}
   * <li>Leave sync on {@link #shutdownExecutors}.
   * </ul>
   */
  private static class ShutdownThread extends Thread {
    private Deque<ExecutorService> shutdownExecutors = new ConcurrentLinkedDeque<>();

    private Object sync = new Object();

    private AtomicInteger numberOfServicesToShutdown = new AtomicInteger(0);

    public ShutdownThread() {
      super("ExecutorManager-shutdown");
      setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error("Uncaught exception in ExecutorManagers shurdownThread. Restart the server.", e);
        }
      });
    }

    @Override
    public void run() {
      while (true) {
        synchronized (sync) {
          try {
            sync.wait(10000);
          } catch (InterruptedException e) {
            // we were interrupted, lets quietly stop.
            return;
          }
        }

        if (numberOfServicesToShutdown.get() > 0) {
          synchronized (shutdownExecutors) {
            // If the "shutdown" request was sent from a thread that would itself be shutdown, we give it a grace period
            // of 100ms to finish its job, before sending the shutdown (and interrupting those threads, which might
            // perhaps lead to exceptions in the logs, although they should not be serious, because the threads should
            // have completed their work already)
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return;
            }
            while (numberOfServicesToShutdown.get() > 0) {
              ExecutorService executor = shutdownExecutors.poll();
              executor.shutdownNow();
              numberOfServicesToShutdown.decrementAndGet();
            }
          }
        }
      }
    }
  }
}
