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

import org.diqube.remote.query.thrift.Ticket;
import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Remove a query.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>none.
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = RemoveQueryJsonCommand.NAME)
public class RemoveQueryJsonCommand implements JsonCommand {

  public static final String NAME = "removeQuery";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  public String qubeId;

  @JsonProperty
  @NotNull
  public String queryId;

  @JsonIgnore
  @Inject
  private AnalysisRegistry registry;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);
    if (analysis == null)
      throw new RuntimeException("Unknown analysis: " + analysisId);

    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Unknown qube: " + qubeId);

    UiQuery query = qube.getQuery(queryId);
    if (query == null)
      throw new RuntimeException("Unknwon query: " + queryId);

    qube.getQueries().remove(query);
  }

}
