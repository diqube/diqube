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

/**
 * A {@link ContinuousConsumer} that accepts Row IDs.
 *
 * This is similar to {@link OverwritingRowIdConsumer}, but this is a {@link ContinuousConsumer}, i.e. with each call to
 * the {@link #consume(Long[])} method there are a few new rowIds provided which are as valid as rowIds provided in
 * earlier calls: The new ones extend the set of the valid rowIds.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(RowIdConsumer.class)
public interface RowIdConsumer extends ContinuousConsumer {
  /**
   * Called when there are a few new row IDs available in the source, which extend the set of valid rowIds.
   * 
   * TODO #8 check if the param should be a List<>
   */
  public void consume(Long[] rowIds);
}
