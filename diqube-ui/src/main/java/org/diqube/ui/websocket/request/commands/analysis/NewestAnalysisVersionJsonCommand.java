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
package org.diqube.ui.websocket.request.commands.analysis;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.db.UiDbProvider;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.AnalysisVersionJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Returns the newest available version of a specific analysis.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisVersionJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = NewestAnalysisVersionJsonCommand.NAME)
public class NewestAnalysisVersionJsonCommand implements JsonCommand {

  @TypeScriptProperty
  public static final String NAME = "newestAnalysisVersion";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String analysisId;

  @JsonIgnore
  @Inject
  public UiDbProvider dbProvider;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException, AuthenticationException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    Long newestVersion = dbProvider.getDb().findNewestAnalysisVersion(analysisId);

    if (newestVersion == null)
      throw new RuntimeException("No newest version for analysis available: " + analysisId);

    resultHandler.sendData(new AnalysisVersionJsonResult(newestVersion));
  }

}
