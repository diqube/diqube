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

import java.util.Collection;
import java.util.UUID;

import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;
import org.diqube.remote.base.thrift.RNodeAddress;

/**
 * A consumer that is informed as soon as a table is fully flattened.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(TableFlattenedConsumer.class)
public interface TableFlattenedConsumer extends OverwritingConsumer {
  /**
   * The required table has been flattened fully and is ready for being queried.
   * 
   * @param flattenId
   *          The ID of the flattening - use this to query the table on the query remotes!
   * @param remoteNodes
   *          The addresses of the remote nodes where the table was flattened.
   */
  public void tableFlattened(UUID flattenId, Collection<RNodeAddress> remoteNodes);

}