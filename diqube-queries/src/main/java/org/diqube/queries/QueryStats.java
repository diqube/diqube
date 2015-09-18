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

/**
 * Data object where statistics about the execution of a query is collected.
 *
 * @author Bastian Gloeckle
 */
public class QueryStats {
  private long startedUntilDoneMs;

  private Map<Integer, Long> stepThreadActiveMs;

  private int numberOfThreads;

  private int numberOfTemporaryColumnShardsCreated;

  private int numberOfTemporaryColumnShardsFromCache;

  private Map<String, Integer> pageAccess;

  private Map<String, Integer> temporaryPageAccess;

  private int numberOfPagesInTable;

  private int numberOfTemporaryPages;

  private Map<String, Integer> numberOfTemporaryVersionsPerColName;

  private String nodeName;

  public QueryStats(String nodeName, long startedUntilDoneMs, Map<Integer, Long> stepThreadActiveMs,
      int numberOfThreads, int numberOfTemporaryColumnShardsCreated, int numberOfTemporaryColumnShardsFromCache,
      Map<String, Integer> pageAccess, Map<String, Integer> temporaryPageAccess, int numberOfPagesInTable,
      int numberOfTemporaryPages, Map<String, Integer> numberOfTemporaryVersionsPerColName) {
    this.nodeName = nodeName;
    this.startedUntilDoneMs = startedUntilDoneMs;
    this.stepThreadActiveMs = stepThreadActiveMs;
    this.numberOfThreads = numberOfThreads;
    this.numberOfTemporaryColumnShardsCreated = numberOfTemporaryColumnShardsCreated;
    this.numberOfTemporaryColumnShardsFromCache = numberOfTemporaryColumnShardsFromCache;
    this.pageAccess = pageAccess;
    this.temporaryPageAccess = temporaryPageAccess;
    this.numberOfPagesInTable = numberOfPagesInTable;
    this.numberOfTemporaryPages = numberOfTemporaryPages;
    this.numberOfTemporaryVersionsPerColName = numberOfTemporaryVersionsPerColName;
  }

  public long getStartedUntilDoneMs() {
    return startedUntilDoneMs;
  }

  public Map<Integer, Long> getStepThreadActiveMs() {
    return stepThreadActiveMs;
  }

  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  public int getNumberOfTemporaryColumnShardsCreated() {
    return numberOfTemporaryColumnShardsCreated;
  }

  public int getNumberOfTemporaryColumnShardsFromCache() {
    return numberOfTemporaryColumnShardsFromCache;
  }

  public Map<String, Integer> getPageAccess() {
    return pageAccess;
  }

  public Map<String, Integer> getTemporaryPageAccess() {
    return temporaryPageAccess;
  }

  public int getNumberOfPagesInTable() {
    return numberOfPagesInTable;
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
