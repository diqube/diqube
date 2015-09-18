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
package org.diqube.remote.cluster;

import org.diqube.queries.QueryStats;
import org.diqube.remote.cluster.thrift.RClusterQueryStatistics;

/**
 *
 * @author Bastian Gloeckle
 */
public class RClusterQueryStatsUtil {
  public static RClusterQueryStatistics createRQueryStats(QueryStats queryStats) {
    RClusterQueryStatistics res = new RClusterQueryStatistics();
    res.setNodeName(queryStats.getNodeName());
    res.setNumberOfTemporaryColumnShardsCreated(queryStats.getNumberOfTemporaryColumnShardsCreated());
    res.setNumberOfTemporaryColumnShardsFromCache(queryStats.getNumberOfTemporaryColumnShardsFromCache());
    res.setNumberOfThreads(queryStats.getNumberOfThreads());
    res.setStartedUntilDoneMs(queryStats.getStartedUntilDoneMs());
    res.setStepThreadActiveMs(queryStats.getStepThreadActiveMs());
    res.setNumberOfPageAccesses(queryStats.getPageAccess());
    res.setNumberOfTemporaryPageAccesses(queryStats.getTemporaryPageAccess());
    res.setNumberOfPagesInTable(queryStats.getNumberOfPagesInTable());
    res.setNumberOfTemporaryPages(queryStats.getNumberOfTemporaryPages());
    res.setNumberOfTemporaryVersionsPerColName(queryStats.getNumberOfTemporaryVersionsPerColName());
    return res;
  }

  public static QueryStats createQueryStats(RClusterQueryStatistics remote) {
    QueryStats res = new QueryStats(remote.getNodeName(), //
        remote.getStartedUntilDoneMs(), //
        remote.getStepThreadActiveMs(), //
        remote.getNumberOfThreads(), //
        remote.getNumberOfTemporaryColumnShardsCreated(), //
        remote.getNumberOfTemporaryColumnShardsFromCache(), //
        remote.getNumberOfPageAccesses(), //
        remote.getNumberOfTemporaryPageAccesses(), //
        remote.getNumberOfPagesInTable(), //
        remote.getNumberOfTemporaryPages(), //
        remote.getNumberOfTemporaryVersionsPerColName());
    return res;
  }
}
