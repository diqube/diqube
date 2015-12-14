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
package org.diqube.server.querymaster.flatten;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.flatten.QueryMasterFlattenService;
import org.diqube.flatten.QueryMasterFlattenService.QueryMasterFlattenCallback;
import org.diqube.permission.TableAccessPermissionUtil;
import org.diqube.remote.query.thrift.FlattenPreparationService;
import org.diqube.remote.query.thrift.RFlattenPreparationException;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.AuthorizationException;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketValidityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link FlattenPreparationService} which allows users to inform the diqube cluster that queries on a
 * specificly flattened table will follow.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenPreparationServiceHandler implements FlattenPreparationService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(FlattenPreparationServiceHandler.class);

  @Inject
  private QueryMasterFlattenService queryMasterFlattenService;

  @Inject
  private TicketValidityService ticketValidityService;

  @Inject
  private TableAccessPermissionUtil tableAccessPermissionUtil;

  @Override
  public void prepareForQueriesOnFlattenedTable(Ticket ticket, String tableName, String flattenBy)
      throws RFlattenPreparationException, TException, AuthenticationException, AuthorizationException {
    ticketValidityService.validateTicket(ticket);

    if (!tableAccessPermissionUtil.hasAccessToTable(ticket, tableName))
      throw new AuthorizationException();

    // start to asynchronously flatten the table, do not care about results.
    queryMasterFlattenService.flattenAsync(tableName, flattenBy, new QueryMasterFlattenCallback() {
      @Override
      public void noNodesServingOriginalTable() {
        logger.trace("Found that no nodes serve table '{}' which should have been flattened.", tableName);
      }

      @Override
      public void flattenException(String msg, Throwable cause) {
        logger.trace("Exception while trying to flatten table '{}' for preparation: {}", tableName, msg, cause);
      }

      @Override
      public void flattenComplete(UUID flattenId, List<RNodeAddress> nodes) {
        logger.trace("Flatten of table '{}' complete.", tableName);
      }
    });
  }

}
