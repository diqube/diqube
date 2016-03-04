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
package org.diqube.execution.consumers;

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;
import org.diqube.function.AggregationFunction;
import org.diqube.function.IntermediaryResult;

/**
 * This {@link ContinuousConsumer} provides intermediary delta updates on calculating the result of a group aggregation
 * function.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(GroupIntermediaryAggregationConsumer.class)
public interface GroupIntermediaryAggregationConsumer extends ContinuousConsumer {
  /**
   * Provide an update.
   * 
   * @param groupId
   *          The group ID this update belongs to.
   * @param colName
   *          The column name this update belongs to.
   * @param oldIntermediaryResult
   *          The previous result that was published by the caller using this method, or <code>null</code> if there is
   *          none. This value is used by the targets in conjunction with newIntermediaryResult to calculate a delta.
   * @param newIntermediaryResult
   *          The currently valid intermediary result returned from the {@link AggregationFunction}.
   */
  public void consumeIntermediaryAggregationResult(long groupId, String colName,
      IntermediaryResult oldIntermediaryResult, IntermediaryResult newIntermediaryResult);
}
