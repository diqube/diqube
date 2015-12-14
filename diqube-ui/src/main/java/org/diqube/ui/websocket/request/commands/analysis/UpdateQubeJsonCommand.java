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

import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.QubeJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Updates the name of a qube.
 * 
 * This command will only update specific fields of the qube itself. To adjust the queries of the qube, see separate
 * commands.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QubeJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = UpdateQubeJsonCommand.NAME)
public class UpdateQubeJsonCommand implements JsonCommand {

  public static final String NAME = "updateQube";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  public String qubeId;

  @JsonProperty
  @NotNull
  public String qubeName;

  @JsonProperty
  @NotNull
  public String sliceId;

  @JsonIgnore
  @Inject
  private AnalysisRegistry analysisRegistry;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = analysisRegistry.getAnalysis(analysisId);
    if (analysis == null)
      throw new RuntimeException("Analysis unknwon: " + analysisId);

    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Qube unknwon: " + qubeId);

    if (qubeName == null || "".equals(qubeName))
      throw new RuntimeException("Qube name empty.");

    if (sliceId == null || "".equals(sliceId))
      throw new RuntimeException("SliceId empty.");

    qube.setName(qubeName);
    qube.setSliceId(sliceId);

    resultHandler.sendData(new QubeJsonResult(qube));
  }

}
