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

import java.util.UUID;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.QueryJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Creates a {@link UiQuery} of a {@link UiQube}.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QueryJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateQueryJsonCommand.NAME)
public class CreateQueryJsonCommand implements JsonCommand {

  public static final String NAME = "createQuery";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  public String qubeId;

  @JsonProperty
  @NotNull
  public String name;

  @JsonProperty
  @NotNull
  public String diql;

  @JsonIgnore
  @Inject
  private AnalysisFactory factory;

  @JsonIgnore
  @Inject
  private AnalysisRegistry registry;

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);

    if (analysis == null)
      throw new RuntimeException("Analysis unknown: " + analysisId);

    UiQube qube = analysis.getQube(qubeId);

    if (qube == null)
      throw new RuntimeException("Qube not found: " + qubeId);

    UiQuery query = factory.createQuery(UUID.randomUUID().toString(), name, diql, UiQuery.DISPLAY_TYPE_TABLE);

    qube.getQueries().add(query);

    resultHandler.sendData(new QueryJsonResult(query));
  }

}
