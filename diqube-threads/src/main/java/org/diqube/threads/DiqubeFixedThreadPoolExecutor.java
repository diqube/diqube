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

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ThreadPoolExecutor} that is aware of the diql Query it is running for.
 *
 * @author Bastian Gloeckle
 */
/* package */class DiqubeFixedThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger logger = LoggerFactory.getLogger(DiqubeFixedThreadPoolExecutor.class);

  private UUID queryUuid;
  private UUID executionUuid;
  private int numberOfThreads;
  private String nameFormat;

  public DiqubeFixedThreadPoolExecutor(int numberOfThreads, ThreadFactory threadFactory, UUID queryUuid,
      UUID executionUuid) {
    super(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        threadFactory);
    this.numberOfThreads = numberOfThreads;
    this.queryUuid = queryUuid;
    this.executionUuid = executionUuid;
  }

  public UUID getQueryUuid() {
    return queryUuid;
  }

  public UUID getExecutionUuid() {
    return executionUuid;
  }

  /* package */void setThreadNameFormatForToString(String nameFormat) {
    this.nameFormat = nameFormat;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[threads=" + numberOfThreads + ",queryUuid=" + queryUuid
        + ",executionUuid=" + executionUuid + ",nameFormat=" + ((nameFormat == null) ? "null" : nameFormat) + "]";
  }

  @Override
  public List<Runnable> shutdownNow() {
    if (logger.isTraceEnabled() && this.getActiveCount() > 0) {
      try {
        // Log current stack trace - when trace is enabled and we will kill some threads, we want to know who did this!
        RuntimeException e = new RuntimeException();
        ByteArrayOutputStream stackTraceStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(stackTraceStream, "UTF-8"));
        e.printStackTrace(writer);
        writer.flush();
        String stackTrace = stackTraceStream.toString("UTF-8");
        logger.trace("Interrupting one executor of query {}, execution {}, stacktrace: {}", queryUuid, executionUuid,
            stackTrace);
      } catch (UnsupportedEncodingException e) {
        logger.trace("Unable to log stack trace of interruption", e);
      }
    }
    return super.shutdownNow();
  }

}
