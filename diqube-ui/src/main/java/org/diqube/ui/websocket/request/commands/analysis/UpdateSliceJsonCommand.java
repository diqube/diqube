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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.diqube.remote.query.thrift.Ticket;
import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.analysis.UiSliceDisjunction;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.SliceJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Updates a slice
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link SliceJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = UpdateSliceJsonCommand.NAME)
public class UpdateSliceJsonCommand implements JsonCommand {

  public static final String NAME = "updateSlice";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  @Valid
  public UiSlice slice;

  @JsonIgnore
  @Inject
  private AnalysisRegistry analysisRegistry;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = analysisRegistry.getAnalysis(analysisId);
    if (analysis == null)
      throw new RuntimeException("Analysis unknwon: " + analysisId);

    UiSlice origSlice = analysis.getSlice(slice.getId());
    if (origSlice == null)
      throw new RuntimeException("Unknown slice: " + slice.getId());

    // validate
    if (slice.getName() == null || "".equals(slice.getName()))
      throw new RuntimeException("Name not set.");

    origSlice.setName(slice.getName());
    origSlice.setManualConjunction(slice.getManualConjunction());
    List<UiSliceDisjunction> newDisjunctions = new ArrayList<>(slice.getSliceDisjunctions());
    origSlice.setSliceDisjunctions(newDisjunctions);

    resultHandler.sendData(new SliceJsonResult(origSlice));
  }

}
