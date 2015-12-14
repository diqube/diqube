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
import javax.validation.constraints.NotNull;

import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Removes a slice.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>none.
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = RemoveSliceJsonCommand.NAME)
public class RemoveSliceJsonCommand implements JsonCommand {

  public static final String NAME = "removeSlice";

  @JsonProperty
  @NotNull
  public String analysisId;

  @JsonProperty
  @NotNull
  public String sliceId;

  @JsonIgnore
  @Inject
  private AnalysisRegistry registry;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);
    if (analysis == null)
      throw new RuntimeException("Unknown analysis: " + analysisId);

    UiSlice slice = analysis.getSlice(sliceId);
    if (slice == null)
      throw new RuntimeException("Unknown slice: " + sliceId);

    // validate if any qube is still using this slice. Do not delete if this is the case!
    List<String> qubeNames = new ArrayList<>();
    for (UiQube qube : analysis.getQubes()) {
      if (qube.getSliceId().equals(slice.getId())) {
        qubeNames.add(qube.getName());
      }
    }

    if (!qubeNames.isEmpty())
      throw new RuntimeException(
          "There is at least one qube that is still using slice '" + slice.getName() + "': " + qubeNames);

    analysis.getSlices().remove(slice);
  }

}
