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
import org.diqube.executionenv.ExecutionEnvironment;

/**
 * A consumer that cosnumes row IDs, but in contrast to the usual {@link RowIdConsumer}, this is an
 * {@link OverwritingConsumer}.
 * 
 * This means that which each call to {@link #consume(ExecutionEnvironment, Long[])} the rowIds provided by previous
 * calls are invalid.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(OverwritingRowIdConsumer.class)
public interface OverwritingRowIdConsumer extends OverwritingConsumer {
  /**
   * Called when there is a new finite set of row IDs available in the source.
   * 
   * @param env
   *          The {@link ExecutionEnvironment} on whichs base the given rowIds were calculated.
   * @param rowIds
   *          The rowIds that are active.
   */
  public void consume(ExecutionEnvironment env, Long[] rowIds);
}
