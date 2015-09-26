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

import javax.inject.Inject;

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.QubeJsonResult;
import org.diqube.ui.websocket.result.analysis.SliceJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Creates a new qube inside an analysis. If there are no slices yet in the analysis or the given slice does not exist,
 * a new slice is added, too.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link SliceJsonResult} (optional; if sent, then this is sent first).
 * <li>{@link QubeJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateQubeJsonCommand.NAME)
public class CreateQubeJsonCommand implements JsonCommand {

  public static final String NAME = "createQube";

  private static final String DEFAULT_SLICE_NAME = "Default";
  private static final String DEFAULT_QUBE_NAME = "Default";

  @JsonProperty
  private String analysisId;

  @JsonProperty(required = false)
  private String sliceName;

  @JsonProperty(required = false)
  private String name;

  @Inject
  @JsonIgnore
  private AnalysisRegistry registry;

  @Inject
  @JsonIgnore
  private AnalysisFactory factory;

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);

    if (sliceName == null)
      sliceName = DEFAULT_SLICE_NAME;

    if (analysis == null)
      throw new RuntimeException("Unknown analysis: " + analysisId);

    UiSlice slice = analysis.getSlice(sliceName);
    if (analysis.getSlices().isEmpty() || slice == null) {
      slice = factory.createSlice(sliceName, new ArrayList<>());
      analysis.getSlices().add(slice);
      resultHandler.sendData(new SliceJsonResult(slice));
    }

    if (name == null)
      name = DEFAULT_QUBE_NAME;

    UiQube qube = factory.createQube(name, sliceName);
    analysis.getQubes().add(qube);
    resultHandler.sendData(new QubeJsonResult(qube));
  }

}