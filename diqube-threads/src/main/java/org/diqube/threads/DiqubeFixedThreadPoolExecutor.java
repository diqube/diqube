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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ThreadPoolExecutor} that is aware of the diql Query it is running for.
 *
 * @author Bastian Gloeckle
 */
/* package */class DiqubeFixedThreadPoolExecutor extends ThreadPoolExecutor {

  private UUID queryUuid;
  private int numberOfThreads;
  private String nameFormat;

  public DiqubeFixedThreadPoolExecutor(int numberOfThreads, ThreadFactory threadFactory, UUID queryUuid) {
    super(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        threadFactory);
    this.numberOfThreads = numberOfThreads;
    this.queryUuid = queryUuid;
  }

  public UUID getQueryUuid() {
    return queryUuid;
  }

  /* package */void setThreadNameFormatForToString(String nameFormat) {
    this.nameFormat = nameFormat;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[threads=" + numberOfThreads + ",queryUuid=" + queryUuid + ",nameFormat="
        + ((nameFormat == null) ? "null" : nameFormat) + "]";
  }

}
