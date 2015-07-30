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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.diqube.queries.QueryStats;
import org.diqube.remote.cluster.thrift.RClusterQueryStatistics;

/**
 *
 * @author Bastian Gloeckle
 */
public class RClusterQueryStatsUtil {
  public static QueryStats createQueryStats(UUID queryUuid, RClusterQueryStatistics remote) {
    QueryStats res = new QueryStats(queryUuid);

    res.setNumberOfThreads(remote.getNumberOfThreads());
    res.setStartedUntilDoneMs(remote.getStartedUntilDoneMs());
    res.setNumberOfTemporaryColumnsCreated(remote.getNumberOfTemporaryColumnsCreated());
    res.setStepThreadActiveMs(new ConcurrentHashMap<>(remote.getStepThreadActiveMs()));
    return res;
  }

  public static RClusterQueryStatistics createRQueryStats(QueryStats queryStats) {
    RClusterQueryStatistics res = new RClusterQueryStatistics();
    res.setNumberOfTemporaryColumnsCreated(queryStats.getNumberOfTemporaryColumnsCreated());
    res.setNumberOfThreads(queryStats.getNumberOfThreads());
    res.setStartedUntilDoneMs(queryStats.getStartedUntilDoneMs());
    res.setStepThreadActiveMs(queryStats.getStepThreadActiveMs());
    return res;
  }
}
