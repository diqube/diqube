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
package org.diqube.ui.websocket.request.commands;

import java.util.List;

import org.apache.thrift.TException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.result.TableNameListJsonResult;

/**
 * Lists all currently available table names in the diqube-server cluster.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link TableNameListJsonResult}
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = ListAllTablesJsonCommand.NAME)
public class ListAllTablesJsonCommand implements JsonCommand {

  public static final String NAME = "listAllTables";

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {

    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    List<String> tables;
    try {
      tables = clusterInteraction.getClusterInformationService().getAvailableTables(ticket);
    } catch (TException e) {
      throw new RuntimeException("Could not get list of tables", e);
    }

    resultHandler.sendData(new TableNameListJsonResult(tables));
  }

}
