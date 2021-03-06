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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.buildinfo.BuildInfo;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.result.VersionJsonResult;

/**
 * Command that returns version information when executed.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link VersionJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = VersionJsonCommand.NAME)
public class VersionJsonCommand implements JsonCommand {
  @TypeScriptProperty
  public static final String NAME = "version";

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    VersionJsonResult res = new VersionJsonResult();
    res.setBuildTimestamp(BuildInfo.getTimestamp());
    res.setGitCommitShort(BuildInfo.getGitCommitShort());
    res.setGitCommitLong(BuildInfo.getGitCommitLong());
    resultHandler.sendData(res);
  }

}
