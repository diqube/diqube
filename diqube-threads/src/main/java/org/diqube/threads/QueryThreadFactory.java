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

import java.util.UUID;
import java.util.concurrent.ThreadFactory;

import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ThreadFactory} that registers an {@link QueryUncaughtExceptionHandler} for each created {@link Thread} and
 * ensures that {@link QueryUuid} is filled correctly.
 *
 * @author Bastian Gloeckle
 */
public class QueryThreadFactory implements ThreadFactory {
  private static final Logger logger = LoggerFactory.getLogger(QueryThreadFactory.class);

  private ThreadFactory delegate;
  private UUID queryUuid;
  private QueryRegistry queryRegistry;

  private UUID executionUuid;

  public QueryThreadFactory(ThreadFactory delegate, UUID queryUuid, UUID executionUuid, QueryRegistry queryRegistry) {
    this.delegate = delegate;
    this.queryUuid = queryUuid;
    this.executionUuid = executionUuid;
    this.queryRegistry = queryRegistry;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread delegateRes = delegate.newThread(new Runnable() {
      @Override
      public void run() {
        QueryUuid.setCurrentQueryUuidAndExecutionUuid(queryUuid, executionUuid);
        try {
          r.run();
        } finally {
          QueryUuid.clearCurrent();
        }
      }
    });

    delegateRes.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        if (queryUuid == null)
          logger.error("Unhandled exception", e);
        else if (!queryRegistry.handleException(queryUuid, executionUuid, e))
          logger.error("Unhandled exception of query that is no longer active (" + queryUuid + ")", e);
      }
    });

    return delegateRes;
  }
}
