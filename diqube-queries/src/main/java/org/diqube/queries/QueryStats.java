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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Data object where statistics about the execution of a query is collected.
 *
 * @author Bastian Gloeckle
 */
public class QueryStats {
  private UUID queryUuid;

  private long startedUntilDoneMs;

  private ConcurrentMap<Integer, Long> stepThreadActiveMs = new ConcurrentHashMap<>();

  private int numberOfThreads;

  private AtomicInteger numberOfTemporaryColumnsCreated = new AtomicInteger(0);

  public QueryStats(UUID queryUuid) {
    this.queryUuid = queryUuid;
  }

  public UUID getQueryUuid() {
    return queryUuid;
  }

  public long getStartedUntilDoneMs() {
    return startedUntilDoneMs;
  }

  public void setStartedUntilDoneMs(long startedUntilDoneMs) {
    this.startedUntilDoneMs = startedUntilDoneMs;
  }

  public ConcurrentMap<Integer, Long> getStepThreadActiveMs() {
    return stepThreadActiveMs;
  }

  public void addStepThreadActiveMs(int stepId, long activeMs) {
    stepThreadActiveMs.merge(stepId, activeMs, (oldActive, newActive) -> oldActive + newActive);
  }

  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  public void setNumberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
  }

  public int getNumberOfTemporaryColumnsCreated() {
    return numberOfTemporaryColumnsCreated.get();
  }

  public void incNumberOfTemporaryColumnsCreated() {
    numberOfTemporaryColumnsCreated.incrementAndGet();
  }

  public void setStepThreadActiveMs(ConcurrentMap<Integer, Long> stepThreadActiveMs) {
    this.stepThreadActiveMs = stepThreadActiveMs;
  }

  public void setNumberOfTemporaryColumnsCreated(int numberOfTemporaryColumnsCreated) {
    this.numberOfTemporaryColumnsCreated.set(numberOfTemporaryColumnsCreated);
  }
}
