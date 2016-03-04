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

import java.util.Map;

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;

/**
 * A {@link ContinuousConsumer} that consumes the values of rows of a specific column.
 *
 * <p>
 * Please note that there might be multiple calls for the same colName/rowId combination. In that case, the last value
 * overwrites the previous ones. This is why this consumer is an {@link OverwritingConsumer}. Please note that this can
 * happen only on the query master, not on the cluster nodes, as they do not use the {@link ColumnVersionBuiltConsumer},
 * but only {@link ColumnBuiltConsumer}s.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(ColumnValueConsumer.class)
public interface ColumnValueConsumer extends ContinuousConsumer {
  /**
   * Called when there are new values of a specific column available
   * 
   * @param values
   *          Mapping from rowID to value
   */
  public void consume(String colName, Map<Long, Object> values);
}
