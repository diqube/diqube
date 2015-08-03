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

import java.util.UUID;
import java.util.stream.Stream;

import org.diqube.util.Pair;

/**
 * Class that can provide the query UUID and execution UUID to any thread while executing a query.
 * 
 * A query UUID is and ID identifying a global query across the whole diqube cluster - meaning the execution of one diql
 * string that was sent by the user. A query can have multiple executions, though - namely one for the query master and
 * then for each query remote another one.
 * 
 * The query and execution UUID currently being active are managed using {@link ThreadLocal}s and the execution engine
 * (=all classes that are used during execution) rely on these fields being available. Therefore, when other threads are
 * used for computing some part of the execution pipelines (e.g. when using parallel {@link Stream}s), you need to make
 * sure to populate the state correctly to those threads (see {@link #getCurrentThreadState()},
 * {@link #setCurrentThreadState(QueryUuidThreadState)} and {@link QueryUuidThreadState}).
 *
 * @author Bastian Gloeckle
 */
public class QueryUuid {
  private static final ThreadLocal<Pair<UUID, UUID>> queryUuidThreadLocal = new ThreadLocal<>();

  /**
   * @param queryUuid
   *          The query UUID for the current thread.
   * @param executionUuid
   *          The execution UUID for the current thread.
   */
  public static void setCurrentQueryUuidAndExecutionUuid(UUID queryUuid, UUID executionUuid) {
    queryUuidThreadLocal.set(new Pair<>(queryUuid, executionUuid));
  }

  /**
   * Call this method to clear the resources taken up from a call to {@link #setCurrentQueryUuidAndExecutionUuid(UUID)}.
   */
  public static void clearCurrent() {
    queryUuidThreadLocal.set(null);
  }

  /**
   * @return The query UUID of the current thread if available, else <code>null</code>. The returned UUID is that query
   *         UUID that the current thread is supposed to work for.
   */
  public static UUID getCurrentQueryUuid() {
    Pair<UUID, UUID> p = queryUuidThreadLocal.get();
    if (p == null)
      return null;
    return p.getLeft();
  }

  /**
   * @return The execution UUID of the current thread if available, else <code>null</code>.
   */
  public static UUID getCurrentExecutionUuid() {
    Pair<UUID, UUID> p = queryUuidThreadLocal.get();
    if (p == null)
      return null;
    return p.getRight();
  }

  /**
   * @return The current state of QueryUuid, dumped in a immutable {@link QueryUuidThreadState} object that can be used
   *         for {@link #setCurrentThreadState(QueryUuidThreadState)} (even multiple calls to the latter are possible).
   * @see class comment on {@link QueryUuid}.
   * @see QueryUuidThreadState
   */
  public static QueryUuidThreadState getCurrentThreadState() {
    return new QueryUuidThreadState(getCurrentQueryUuid(), getCurrentExecutionUuid());
  }

  /**
   * @param state
   *          (Re-)set a specific state in the current thread. Be sure to call {@link #clearCurrent()} after calling
   *          this method on a thread!
   * @see class comment on {@link QueryUuid}.
   * @see QueryUuidThreadState
   */
  public static void setCurrentThreadState(QueryUuidThreadState state) {
    setCurrentQueryUuidAndExecutionUuid(state.queryUuid, state.executionUuid);
  }

  /**
   * Immutable state that was collected from a Thread using {@link QueryUuid#getCurrentThreadState()}.
   * 
   * This is useful when using parallel streams for example: We need to maintain the threadlocals used in
   * {@link QueryUuid} for all threads that are computing some part of an execution pipeline. If parallel {@link Stream}
   * s are used, these may use different threads than the one currently being executed. Therefore, before starting a new
   * parallel stream processing pipeline, one should fetch the current UUIDs using
   * {@link QueryUuid#getCurrentThreadState()}. Then, in each step in the pipeline that calls other classes that might
   * rely on the UUIDs being set, the {@link QueryUuid#setCurrentThreadState(QueryUuidThreadState)} should be called, to
   * prepare the ThreadLocals on that thread. After that computation method is completed, the thread state should be
   * cleared using {@link QueryUuid#clearCurrent()}. After the pipeline has completed, be sure to re-set the thread
   * state once using {@link QueryUuid#setCurrentThreadState(QueryUuidThreadState)}, as the pipeline might have computed
   * some parts in the current thread (= it did not only use the threads in a ThreadPool to compute the pipeline, but
   * the current thread, too) - and then {@link QueryUuid#clearCurrent()} will have been called on that thread.
   * 
   * Example:
   * 
   * <pre>
   * QueryUuidThreadState uuidState = QueryUuid.getCurrentThreadState();
   * try {
   *   Stream.parallel().map(a -> {
   *     QueryUuid.setCurrentThreadState(uuidState);
   *     try {
   *       // do your mapping stuff
   *     finally {
   *       QueryUuid.clearCurrent();
   *     }
   *   };).findAny();
   * } finally {
   *   QueryUuid.setCurrentThreadState(uuidState);
   * }
   * 
   * </pre>
   */
  public static class QueryUuidThreadState {
    private UUID queryUuid;
    private UUID executionUuid;

    private QueryUuidThreadState(UUID queryUuid, UUID executionUuid) {
      this.queryUuid = queryUuid;
      this.executionUuid = executionUuid;
    }
  }

}
