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

import java.util.List;
import java.util.Map;

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;

/**
 * This {@link ContinuousConsumer} is called as soon as the groups for a new set of Row IDs is available. This means
 * that this consumer provides deltas, in contrast to a {@link GroupConsumer}, which provides the full grouping
 * information on each call.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(GroupDeltaConsumer.class)
public interface GroupDeltaConsumer extends ContinuousConsumer {
  /**
   * Is called as soon as there is new grouping data available.
   * 
   * @param lastChangedGroups
   *          Map from an identifying row ID (="group ID") to array of RowIds that have been found to be inside that
   *          group in the last grouping run. <b>The contents of this map MUST NOT be changed in any way!</b>
   */
  public void consumeGroupDeltas(Map<Long, List<Long>> lastChangedGroups);
}
