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
package org.diqube.metadata;

import java.util.Set;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterLayout;
import org.diqube.consensus.ConsensusClient;
import org.diqube.consensus.ConsensusClient.ClosableProvider;
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.metadata.consensus.TableMetadataStateMachine;
import org.diqube.metadata.consensus.TableMetadataStateMachine.GetTableMetadata;
import org.diqube.permission.TableAccessPermissionUtil;
import org.diqube.remote.query.thrift.ROptionalTableMetadata;
import org.diqube.remote.query.thrift.TableMetadataService;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for retrieving {@link TableMetadata} of a specific table, if metadata is available.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableMetadataHandler implements TableMetadataService.Iface {
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataHandler.class);

  @Inject
  private TableAccessPermissionUtil tableAccessPermissionUtil;

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private ClusterLayout clusterLayout;

  @Inject
  private ConsensusClient consensusClient;

  @Override
  public ROptionalTableMetadata getTableMetadata(Ticket ticket, String tableName)
      throws AuthenticationException, AuthorizationException, TException {
    ticketValidityService.validateTicket(ticket);

    if (!tableAccessPermissionUtil.hasAccessToTable(ticket, tableName))
      throw new AuthorizationException("No access to table or table does not exist.");

    Set<String> allTables;
    try {
      allTables = clusterLayout.getAllTablesServed();
    } catch (InterruptedException | ConsensusClusterUnavailableException e) {
      logger.warn("Interrupted while loading list of all tables", e);
      return new ROptionalTableMetadata();
    }
    if (!allTables.contains(tableName))
      throw new AuthorizationException("No access to table or table does not exist.");

    try (ClosableProvider<TableMetadataStateMachine> p =
        consensusClient.getStateMachineClient(TableMetadataStateMachine.class)) {

      Pair<TableMetadata, Long> resPair = p.getClient().getTableMetadata(GetTableMetadata.local(tableName));
      if (resPair == null || resPair.getLeft() == null)
        return new ROptionalTableMetadata();

      ROptionalTableMetadata res = new ROptionalTableMetadata();
      res.setTableMetadata(resPair.getLeft());
      return res;
    } catch (ConsensusClusterUnavailableException e) {
      logger.warn("Could not find table metadata of table '{}' because consensus cluster is unavailable", e);
      return new ROptionalTableMetadata();
    }
  }

}
