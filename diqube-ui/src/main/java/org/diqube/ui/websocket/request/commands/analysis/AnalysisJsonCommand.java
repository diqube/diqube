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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.db.UiDbProvider;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.AnalysisJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Returns a specific analysis, no matter if it belongs to the tickets user or not.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = AnalysisJsonCommand.NAME)
public class AnalysisJsonCommand implements JsonCommand {

  @TypeScriptProperty
  public static final String NAME = "analysis";

  @JsonProperty
  @TypeScriptProperty
  public String analysisId;

  // if null, load newest version.
  @JsonProperty
  @TypeScriptProperty(optional = true)
  public Long analysisVersion;

  @JsonIgnore
  @Inject
  public UiDbProvider dbProvider;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException, AuthenticationException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    if (analysisVersion == null) {
      analysisVersion = dbProvider.getDb().findNewestAnalysisVersion(analysisId);

      if (analysisVersion == null)
        throw new RuntimeException("Analysis not found: " + analysisId);
    }

    UiAnalysis analysis = dbProvider.getDb().loadAnalysisVersion(analysisId, analysisVersion);

    if (analysis == null)
      throw new RuntimeException("Analysis version not found: " + analysisId + " version " + analysisVersion);

    resultHandler.sendData(new AnalysisJsonResult(analysis));
  }

}
