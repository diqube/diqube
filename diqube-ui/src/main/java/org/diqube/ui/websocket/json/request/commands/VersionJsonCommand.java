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

import org.diqube.buildinfo.BuildInfo;
import org.diqube.ui.websocket.json.request.CommandClusterInteraction;
import org.diqube.ui.websocket.json.request.CommandResultHandler;
import org.diqube.ui.websocket.json.result.VersionJsonResult;

/**
 * Command that returns version information when executed.
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = VersionJsonCommand.NAME)
public class VersionJsonCommand implements JsonCommand {
  public static final String NAME = "version";

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    VersionJsonResult res = new VersionJsonResult();
    res.setBuildTimestamp(BuildInfo.getTimestamp());
    res.setGitCommitShort(BuildInfo.getGitCommitShort());
    res.setGitCommitLong(BuildInfo.getGitCommitLong());
    resultHandler.sendData(res);
  }

}
