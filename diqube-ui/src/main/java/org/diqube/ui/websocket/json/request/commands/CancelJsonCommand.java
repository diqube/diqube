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
package org.diqube.ui.websocket.json.request.commands;

import org.diqube.ui.websocket.json.request.CommandClusterInteraction;
import org.diqube.ui.websocket.json.request.CommandResultHandler;

/**
 * Cancels the execution of the request that was executed with the same reuqestId.
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CancelJsonCommand.NAME)
public class CancelJsonCommand implements JsonCommand {
  public static final String NAME = "cancel";

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    // currently only queries can be cancelled.
    clusterInteraction.cancelQuery();
  }
}
