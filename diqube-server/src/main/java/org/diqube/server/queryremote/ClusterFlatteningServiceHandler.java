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
package org.diqube.server.queryremote;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterFlatteningService;
import org.diqube.remote.cluster.thrift.RFlattenException;

/**
 * Handler for {@link ClusterFlatteningService}, which handles flattening local tables.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterFlatteningServiceHandler implements ClusterFlatteningService.Iface {

  @Override
  public void flattenAllLocalShards(String tableName, String flattenBy, List<RNodeAddress> otherFlatteners,
      RNodeAddress resultAddress) throws RFlattenException, TException {
  }

  @Override
  public void shardsFlattened(String tableName, String flattenBy,
      Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRows, RNodeAddress flattener) throws TException {
  }

  @Override
  public void flatteningDone(String tableName, String flattenBy, RNodeAddress flattener) throws TException {
  }

}
