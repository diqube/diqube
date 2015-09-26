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
package org.diqube.cluster;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.query.thrift.ClusterInformationService;

/**
 * Handler for the service that clients can use to get information about the cluster of diqube-servers.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterInformationServiceHandler implements ClusterInformationService.Iface {

  @Inject
  private ClusterManager clusterManager;

  /**
   * Lists the names of the tables that are currently available in the diqube-server cluster.
   */
  @Override
  public List<String> getAvailableTables() throws TException {
    // TODO #48: We should not use the ClusterLayout for this, but the new to-be-created information on table data.
    return new ArrayList<>(clusterManager.getClusterLayout().getAllTablesServed());
  }
}
