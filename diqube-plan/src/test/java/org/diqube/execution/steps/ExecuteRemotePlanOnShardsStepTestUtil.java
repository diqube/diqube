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
package org.diqube.execution.steps;

import java.util.List;
import java.util.function.Function;

import org.diqube.data.TableShard;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep.RemotePlanBuilder;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.util.Triple;

/**
 * Test util for {@link ExecuteRemotePlanOnShardsStep} which makes the package-private methods accessible by tests.
 * 
 * <p>
 * This is not in diqube-execution, as all tests testing diqube-execution are in diqube-plan.
 *
 * @author Bastian Gloeckle
 */
public class ExecuteRemotePlanOnShardsStepTestUtil {
  /**
   * Replaces the default strategy on how to create a {@link ExecutablePlan} out of a {@link RExecutionPlan} that is
   * used by the given {@link ExecuteRemotePlanOnShardsStep}.
   * 
   * @param providerFn
   *          Given a {@link RExecutionPlan} and the result {@link GroupIntermediaryAggregationConsumer} and
   *          {@link ColumnValueConsumer}s that the {@link ExecuteRemotePlanOnShardsStep} wants to get filled with the
   *          results, this function returns a list of {@link ExecutablePlan}s, typically one for each
   *          {@link TableShard} of the affected table.
   */
  public static void addCustomRemoteExecution(ExecuteRemotePlanOnShardsStep step,
      Function<Triple<RExecutionPlan, GroupIntermediaryAggregationConsumer, ColumnValueConsumer>, List<ExecutablePlan>> providerFn) {
    step.setRemotePlanBuilder(new RemotePlanBuilder() {
      @Override
      public List<ExecutablePlan> build(RExecutionPlan remotePlan,
          GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer,
          ColumnValueConsumer columnValueConsumer) {
        return providerFn.apply(new Triple<RExecutionPlan, GroupIntermediaryAggregationConsumer, ColumnValueConsumer>(
            remotePlan, groupIntermediaryAggregationConsumer, columnValueConsumer));
      }
    });
  }
}
