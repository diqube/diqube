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
package org.diqube.threads.test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.diqube.queries.QueryUuid;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides {@link Executor}s that can be used in tests that execute diql queries and need a thread pool to execute
 * ExecutablePlans.
 *
 * TODO move this someplace else without introducing cyclic dependencies.
 *
 * @author Bastian Gloeckle
 */
public class TestExecutors {
  private static final Logger logger = LoggerFactory.getLogger(TestExecutors.class);

  private ExecutorManager executorManager;

  public TestExecutors(ExecutorManager executorManager) {
    this.executorManager = executorManager;
  }

  public ExecutorService newTestExecutor(int numberOfThreads) {
    RuntimeException e = new RuntimeException();
    String testMethod = e.getStackTrace()[1].getMethodName();

    UUID queryUuid = QueryUuid.getCurrentQueryUuid();
    UUID executionUuid = QueryUuid.getCurrentExecutionUuid();

    logger.info("Test method {}: Query {}, Execution {}", testMethod, queryUuid, executionUuid);

    ExecutorService res = (ExecutorService) executorManager.newQueryFixedThreadPool(numberOfThreads + 1,
        queryUuid + "#" + executionUuid + "#" + testMethod + "-%d", queryUuid, executionUuid);

    res.execute(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          return;
        }

        logger.error("Killing test after 60s: query {} execution {}", queryUuid, executionUuid);

        res.shutdownNow();
      }
    });

    return res;
  }
}
