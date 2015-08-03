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
package org.diqube.server.util;

import java.util.List;
import java.util.Map.Entry;

import org.diqube.data.TableShard;
import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.queries.QueryStats;
import org.diqube.queries.QueryStatsManager;

/**
 * Util for publishsing some {@link QueryStats} after execution of an {@link ExecutablePlan} has been completed.
 *
 * @author Bastian Gloeckle
 */
public class ExecutablePlanQueryStatsUtil {

  /**
   * Publish some query stats.
   * 
   * This method can be called multiple times - for each {@link ExecutablePlan} of a {@link TableShard} once for example
   * - the values will be summed up accordingly.
   * 
   * @param queryStats
   *          The {@link QueryStats} object that should be adjusted
   * @param plan
   *          The plan that has been executed.
   */
  public void publishQueryStats(QueryStatsManager queryStats, ExecutablePlan plan) {
    ExecutionEnvironment lastEnv;
    ColumnVersionManager columnVersionManager = plan.getColumnVersionManager();
    if (columnVersionManager != null && columnVersionManager.getLastVersionedExecutionEnvironment() != null)
      lastEnv = columnVersionManager.getLastVersionedExecutionEnvironment();
    else
      lastEnv = plan.getDefaultExecutionEnvironment();

    int numberOfPages = queryStats.getNumberOfPages() + //
        lastEnv.getAllNonTemporaryColumnShards().values().stream()
            .filter(colShard -> lastEnv.getPureStandardColumnShard(colShard.getName()) != null)
            .mapToInt(colShard -> lastEnv.getPureStandardColumnShard(colShard.getName()).getPages().size()).sum();
    queryStats.setNumberOfPages(numberOfPages);

    int numberOfTempPages = queryStats.getNumberOfTemporaryPages()
        + lastEnv.getAllTemporaryColumnShards().values().stream().flatMap(listOfPages -> listOfPages.stream())
            .filter(colShard -> lastEnv.getPureStandardColumnShard(colShard.getName()) != null)
            .mapToInt(colShard -> lastEnv.getPureStandardColumnShard(colShard.getName()).getPages().size()).sum();
    queryStats.setNumberOfTemporaryPages(numberOfTempPages);

    for (Entry<String, List<QueryableColumnShard>> tempE : lastEnv.getAllTemporaryColumnShards().entrySet()) {
      int numberOfTempVersions =
          queryStats.getNumberOfTemporaryVersionsPerColName().getOrDefault(tempE.getKey(), 0) + tempE.getValue().size();
      queryStats.setNumberOfTemporaryVersionsOfColumn(tempE.getKey(), numberOfTempVersions);
    }
  }
}
