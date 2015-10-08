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
import java.util.UUID;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.AnalysisFactory;
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
 * Creates a {@link UiSlice}.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link SliceJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateSliceJsonCommand.NAME)
public class CreateSliceJsonCommand implements JsonCommand {

  public static final String NAME = "createSlice";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  public String name;

  @JsonProperty
  @NotNull
  public String manualConjunction;

  @JsonProperty
  @NotNull
  @Valid
  public List<UiSliceDisjunction> sliceDisjunctions;

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

    UiSlice slice = factory.createSlice(UUID.randomUUID().toString(), name, new ArrayList<>());
    slice.setManualConjunction(manualConjunction);
    slice.getSliceDisjunctions().addAll(sliceDisjunctions);

    analysis.getSlices().add(slice);

    resultHandler.sendData(new SliceJsonResult(slice));
  }

}
