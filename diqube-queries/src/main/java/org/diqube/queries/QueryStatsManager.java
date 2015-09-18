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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.data.colshard.ColumnPage;

/**
 * Collects data about a query execution to later be able to create a {@link QueryStats}.
 * 
 * Each query typically has an instance of this {@link QueryStatsManager} to collects its stats. That instance is
 * available through {@link QueryRegistry}.
 *
 * @author Bastian Gloeckle
 */
public class QueryStatsManager {
  private long startedNanos = 0;

  private long completedNanos = 0;

  private ConcurrentMap<Integer, Long> stepThreadActiveMs = new ConcurrentHashMap<>();

  private int numberOfThreads = 0;

  private AtomicInteger numberOfTemporaryColumnShardsCreated = new AtomicInteger(0);

  private AtomicInteger numberOfTemporaryColumnShardsFromCache = new AtomicInteger(0);

  private Map<Integer, AtomicInteger> pageAccess = new ConcurrentHashMap<>();

  private Map<Integer, AtomicInteger> temporaryPageAccess = new ConcurrentHashMap<>();

  private Map<Integer, String> pageNames = new ConcurrentHashMap<>();

  private int numberOfPagesInTable = 0;

  private int numberOfTemporaryPages = 0;

  private Map<String, Integer> numberOfTemporaryVersionsPerColName = new ConcurrentHashMap<>();

  private String nodeName;

  public QueryStatsManager(String nodeName) {
    this.nodeName = nodeName;
  }

  public void addStepThreadActiveMs(int stepId, long activeMs) {
    stepThreadActiveMs.merge(stepId, activeMs, (oldActive, newActive) -> oldActive + newActive);
  }

  public void setNumberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
  }

  public void incNumberOfTemporaryColumnShardsCreated() {
    numberOfTemporaryColumnShardsCreated.incrementAndGet();
  }

  public void incNumberOfTemporaryColumnShardsFromCache() {
    numberOfTemporaryColumnShardsFromCache.incrementAndGet();
  }

  public void setStepThreadActiveMs(ConcurrentMap<Integer, Long> stepThreadActiveMs) {
    this.stepThreadActiveMs = stepThreadActiveMs;
  }

  public void setNumberOfTemporaryColumnShardsCreated(int numberOfTemporaryColumnShardsCreated) {
    this.numberOfTemporaryColumnShardsCreated.set(numberOfTemporaryColumnShardsCreated);
  }

  public void setNumberOfTemporaryColumnShardsFromCache(int numberOfTemporaryColumnShardsFromCache) {
    this.numberOfTemporaryColumnShardsFromCache.set(numberOfTemporaryColumnShardsFromCache);
  }

  public void registerPageAccess(ColumnPage page, boolean isTempColumn) {
    Integer objectId = System.identityHashCode(page);
    pageNames.putIfAbsent(objectId, page.getName());
    if (isTempColumn) {
      if (!temporaryPageAccess.containsKey(objectId)) {
        synchronized (temporaryPageAccess) {
          if (!temporaryPageAccess.containsKey(objectId))
            temporaryPageAccess.put(objectId, new AtomicInteger(0));
        }
      }

      temporaryPageAccess.get(objectId).incrementAndGet();
    } else {
      if (!pageAccess.containsKey(objectId)) {
        synchronized (pageAccess) {
          if (!pageAccess.containsKey(objectId))
            pageAccess.put(objectId, new AtomicInteger(0));
        }
      }

      pageAccess.get(objectId).incrementAndGet();
    }
  }

  public void setNumberOfPages(int numberOfPages) {
    this.numberOfPagesInTable = numberOfPages;
  }

  public void setNumberOfTemporaryPages(int numberOfTemporaryPages) {
    this.numberOfTemporaryPages = numberOfTemporaryPages;
  }

  public int getNumberOfPages() {
    return numberOfPagesInTable;
  }

  public void setNumberOfTemporaryVersionsOfColumn(String colName, int value) {
    numberOfTemporaryVersionsPerColName.put(colName, value);
  }

  public QueryStats createQueryStats() {
    Map<String, Integer> pageAccess = new HashMap<>();
    for (Entry<Integer, AtomicInteger> pageAccessEntry : this.pageAccess.entrySet())
      pageAccess.put(pageNames.get(pageAccessEntry.getKey()), pageAccessEntry.getValue().get());

    Map<String, Integer> temporaryPageAccess = new HashMap<>();
    for (Entry<Integer, AtomicInteger> tempPageAccessEntry : this.temporaryPageAccess.entrySet())
      temporaryPageAccess.put(pageNames.get(tempPageAccessEntry.getKey()), tempPageAccessEntry.getValue().get());

    long startedUntilDoneMs = (long) ((completedNanos - startedNanos) / 1e6);

    return new QueryStats(nodeName, startedUntilDoneMs, new HashMap<>(stepThreadActiveMs), numberOfThreads,
        numberOfTemporaryColumnShardsCreated.get(), numberOfTemporaryColumnShardsFromCache.get(), pageAccess, temporaryPageAccess,
        numberOfPagesInTable, numberOfTemporaryPages, numberOfTemporaryVersionsPerColName);
  }

  public void setStartedNanos(long startedNanos) {
    this.startedNanos = startedNanos;
  }

  public void setCompletedNanos(long completedNanos) {
    this.completedNanos = completedNanos;
  }

  public int getNumberOfTemporaryPages() {
    return numberOfTemporaryPages;
  }

  public Map<String, Integer> getNumberOfTemporaryVersionsPerColName() {
    return numberOfTemporaryVersionsPerColName;
  }

  public String getNodeName() {
    return nodeName;
  }

}
