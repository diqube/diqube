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
package org.diqube.server.querymaster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.execution.ExecutablePlan;
import org.diqube.queries.QueryStats;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RQueryStatisticsDetails;

/**
 * Merges the statistics from various query remotes and the query master into a final {@link RQueryStatistics} object
 * which can be presented to the user.
 *
 * @author Bastian Gloeckle
 */
public class MasterQueryStatisticsMerger {
  private ExecutablePlan masterPlan;
  private RExecutionPlan remotePlan;

  public MasterQueryStatisticsMerger(ExecutablePlan masterPlan, RExecutionPlan remotePlan) {
    this.masterPlan = masterPlan;
    this.remotePlan = remotePlan;
  }

  public RQueryStatistics merge(QueryStats masterStats, List<QueryStats> remoteStats) {
    RQueryStatistics res = new RQueryStatistics();
    Map<Integer, String> stepDescription = new HashMap<>();
    masterPlan.getSteps().forEach(s -> stepDescription.put(s.getStepId(), s.toString()));

    res.setMaster(createDetails(stepDescription, masterStats));

    Map<Integer, String> remoteStepDescription = new HashMap<>();
    remotePlan.getSteps().forEach(s -> remoteStepDescription.put(s.getStepId(), s.toString()));

    List<RQueryStatisticsDetails> remoteDetails = new ArrayList<>();
    for (QueryStats remote : remoteStats)
      remoteDetails.add(createDetails(remoteStepDescription, remote));

    res.setRemotes(remoteDetails);
    return res;
  }

  private RQueryStatisticsDetails createDetails(Map<Integer, String> stepDescription, QueryStats stats) {
    RQueryStatisticsDetails res = new RQueryStatisticsDetails();

    res.setStartedUntilDoneMs(stats.getStartedUntilDoneMs());
    res.setNumberOfThreads(stats.getNumberOfThreads());
    res.setNumberOfTemporaryColumnsCreated(stats.getNumberOfTemporaryColumnsCreated());

    Map<String, Long> timesInSteps = new HashMap<>();
    for (int stepId : stats.getStepThreadActiveMs().keySet())
      timesInSteps.put(stepDescription.get(stepId), stats.getStepThreadActiveMs().get(stepId));
    res.setStepsActiveMs(timesInSteps);

    res.setNumberOfPageAccesses(stats.getPageAccess());
    res.setNumberOfTemporaryPageAccesses(stats.getTemporaryPageAccess());
    res.setNumberOfPages(stats.getNumberOfPages());
    res.setNumberOfTemporaryPages(stats.getNumberOfTemporaryPages());
    res.setNumberOfTemporaryVersionsPerColName(stats.getNumberOfTemporaryVersionsPerColName());

    return res;
  }
}
