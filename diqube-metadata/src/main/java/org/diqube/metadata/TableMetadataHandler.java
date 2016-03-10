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
import org.diqube.consensus.ConsensusClient.ConsensusClusterUnavailableException;
import org.diqube.context.AutoInstatiate;
import org.diqube.name.FlattenedTableNameUtil;
import org.diqube.permission.TableAccessPermissionUtil;
import org.diqube.remote.query.thrift.ROptionalTableMetadata;
import org.diqube.remote.query.thrift.TableMetadataService;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;
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
  private FlattenedTableNameUtil flattenedTableNameUtil;

  @Inject
  private TableMetadataManager tableMetadataManager;

  /**
   * @param tableName
   *          Name of the table to get the metadata of. Can either be a normal tablename, or the name of a flattened
   *          table. The latter can either be a full one (with the flatten ID = one created by
   *          {@link FlattenedTableNameUtil}) or one without the ID (such as the ones used in diql queries; in this
   *          case, the newest locally available flattening will be used).
   */
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
    if (flattenedTableNameUtil.isFlattenedTableName(tableName)) {
      if (!allTables.contains(flattenedTableNameUtil.getOriginalTableNameFromFlatten(tableName)))
        throw new AuthorizationException("No access to table or table does not exist.");
    } else if (!allTables.contains(tableName))
      throw new AuthorizationException("No access to table or table does not exist.");

    TableMetadata metadata = tableMetadataManager.getCurrentTableMetadata(tableName);

    if (metadata == null)
      return new ROptionalTableMetadata();

    // overwrite the tableName by the one that was provided - in case tableMetadataManager extended the tablename of a
    // flattened table by the flattenId, we do not want to leak that!
    metadata.setTableName(tableName);
    ROptionalTableMetadata res = new ROptionalTableMetadata();
    res.setTableMetadata(metadata);
    return res;
  }

}
