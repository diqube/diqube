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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.diqube.context.AutoInstatiate;

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
  private Map<UUID, QueryExceptionHandler> exceptionHandlers = new ConcurrentHashMap<>();

  /**
   * Register a query and its exception handler. Note that for the query {@link #unregisterQuery(UUID)} has to be
   * called.
   */
  public void registerQuery(UUID queryUuid, QueryExceptionHandler exceptionHandler) {
    exceptionHandlers.put(queryUuid, exceptionHandler);
  }

  /**
   * Unregisters a query, all exceptions that will be thrown in one of the queries {@link ExecutorService}s in the
   * future will not be passed on to the registered {@link QueryExceptionHandler} any more!
   */
  public void unregisterQuery(UUID queryUuid) {
    exceptionHandlers.remove(queryUuid);
  }

  /**
   * Call if an exception occurred while executing a given query.
   * 
   * @return <code>true</code> when the exception was handled by an exception handler, <code>false</code> otherwise.
   */
  public boolean handleException(UUID queryUuid, Throwable t) {
    QueryExceptionHandler handler = exceptionHandlers.get(queryUuid);

    if (handler == null)
      return false;

    handler.handleException(t);
    unregisterQuery(queryUuid);
    return true;
  }

  /**
   * Handle and exception that occurred in a thread while executing a query.
   */
  public static interface QueryExceptionHandler {
    /**
     * Handle the given exception. The query will automatically be unregistered in the {@link QueryRegistry}.
     */
    public void handleException(Throwable t);
  }
}
