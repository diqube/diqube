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
package org.diqube.server;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayout;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.permission.TableAccessPermissionUtil;
import org.diqube.remote.query.thrift.ClusterInformationService;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;

/**
 * Handler for the service that clients can use to get information about the cluster of diqube-servers.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterInformationServiceHandler implements ClusterInformationService.Iface {

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private TableAccessPermissionUtil tableAccessPermissionUtil;

  @Inject
  private TicketValidityService ticketValidityService;

  /**
   * Lists the names of the tables that are currently available in the diqube-server cluster.
   */
  @Override
  public List<String> getAvailableTables(Ticket ticket) throws TException, AuthenticationException {
    ticketValidityService.validateTicket(ticket);

    try {
      // TODO #48: We should not use the ClusterLayout for this, but the new to-be-created information on table data.
      Set<String> allTables = clusterLayout.getAllTablesServed();
      List<String> tablesWithAccess = allTables.stream()
          .filter(s -> tableAccessPermissionUtil.hasAccessToTable(ticket, s)).collect(Collectors.toList());
      return tablesWithAccess;
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    } catch (ConsensusClusterUnavailableException e) {
      throw new RuntimeException("Consensus cluster unavailable", e);
    }
  }
}
