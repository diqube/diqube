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
import java.util.stream.Collectors;

import org.diqube.execution.ExecutablePlan;
import org.diqube.queries.QueryStats;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.query.thrift.RQueryStatistics;

import com.google.common.collect.Iterables;

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

    res.setMasterStartedUntilDoneMs(masterStats.getStartedUntilDoneMs());
    List<Long> remotesStartedUntilDone =
        remoteStats.stream().map(stat -> stat.getStartedUntilDoneMs()).collect(Collectors.toList());
    res.setRemotesStartedUntilDoneMs(remotesStartedUntilDone);
    res.setRemoteNumberOfThreads(Iterables.getFirst(remoteStats, null).getNumberOfThreads());

    int numberOfRemoteTempColsCreated =
        (int) remoteStats.stream().mapToLong(stat -> stat.getNumberOfTemporaryColumnsCreated()).sum();
    res.setNumberOfTemporaryColumnsCreated(
        numberOfRemoteTempColsCreated + masterStats.getNumberOfTemporaryColumnsCreated());

    Map<Integer, String> masterStepDescription = new HashMap<>();
    masterPlan.getSteps().forEach(s -> masterStepDescription.put(s.getStepId(), s.toString()));
    Map<String, Long> timesInMasterSteps = new HashMap<>();
    for (int stepId : masterStats.getStepThreadActiveMs().keySet())
      timesInMasterSteps.put(masterStepDescription.get(stepId), masterStats.getStepThreadActiveMs().get(stepId));

    res.setMasterStepsMs(timesInMasterSteps);

    Map<Integer, String> remotesStepDescription = new HashMap<>();
    remotePlan.getSteps().forEach(s -> remotesStepDescription.put(s.getStepId(), s.toString()));
    Map<String, List<Long>> timesInRemoteSteps = new HashMap<>();
    for (QueryStats curRemoteStats : remoteStats) {
      for (int stepId : curRemoteStats.getStepThreadActiveMs().keySet()) {
        String desc = remotesStepDescription.get(stepId);
        if (!timesInRemoteSteps.containsKey(desc))
          timesInRemoteSteps.put(desc, new ArrayList<>());
        timesInRemoteSteps.get(desc).add(curRemoteStats.getStepThreadActiveMs().get(stepId));
      }
    }

    res.setRemotesStepsMs(timesInRemoteSteps);

    return res;
  }
}
