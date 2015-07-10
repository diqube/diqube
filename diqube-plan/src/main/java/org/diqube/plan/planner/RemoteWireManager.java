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
package org.diqube.plan.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupConsumer;
import org.diqube.execution.consumers.GroupDeltaConsumer;
import org.diqube.execution.consumers.GroupFinalAggregationConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.OrderedRowIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepDataType;

/**
 * Simple {@link WireManager} for remote steps.
 *
 * @author Bastian Gloeckle
 */
public class RemoteWireManager implements WireManager<RExecutionPlanStep> {

  private static final Map<Class<? extends GenericConsumer>, RExecutionPlanStepDataType> consumerClassToRemoteDataType =
      new HashMap<>();

  @Override
  public void wire(Class<? extends GenericConsumer> type, RExecutionPlanStep sourceStep, RExecutionPlanStep destStep) {
    if (sourceStep.getProvideDataForSteps() == null
        || !sourceStep.getProvideDataForSteps().containsKey(destStep.getStepId()))
      sourceStep.putToProvideDataForSteps(destStep.getStepId(), new ArrayList<RExecutionPlanStepDataType>());
    sourceStep.getProvideDataForSteps().get(destStep.getStepId()).add(consumerClassToRemoteDataType.get(type));
  }

  static {
    // Keep in sync with ExecutablePlanFromRemoteBuilder
    consumerClassToRemoteDataType.put(ColumnBuiltConsumer.class, RExecutionPlanStepDataType.COLUMN_BUILT);
    consumerClassToRemoteDataType.put(ColumnDictIdConsumer.class, RExecutionPlanStepDataType.COLUMN_DICT_ID);
    consumerClassToRemoteDataType.put(ColumnValueConsumer.class, RExecutionPlanStepDataType.COLUMN_VALUE);
    consumerClassToRemoteDataType.put(GroupConsumer.class, RExecutionPlanStepDataType.GROUP);
    consumerClassToRemoteDataType.put(GroupDeltaConsumer.class, RExecutionPlanStepDataType.GROUP_DELTA);
    consumerClassToRemoteDataType.put(GroupFinalAggregationConsumer.class, RExecutionPlanStepDataType.GROUP_FINAL_AGG);
    consumerClassToRemoteDataType.put(GroupIntermediaryAggregationConsumer.class,
        RExecutionPlanStepDataType.GROUP_INTERMEDIARY_AGG);
    consumerClassToRemoteDataType.put(OrderedRowIdConsumer.class, RExecutionPlanStepDataType.ORDERED_ROW_ID);
    consumerClassToRemoteDataType.put(RowIdConsumer.class, RExecutionPlanStepDataType.ROW_ID);
  }
}
