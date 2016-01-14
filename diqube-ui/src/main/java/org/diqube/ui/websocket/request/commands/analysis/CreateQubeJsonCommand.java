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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.QubeJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Creates a new qube inside an analysis.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QubeJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateQubeJsonCommand.NAME)
public class CreateQubeJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "createQube";

  @JsonProperty
  @TypeScriptProperty
  private String sliceId;

  @JsonProperty
  @TypeScriptProperty
  private String name;

  @Inject
  @JsonIgnore
  private AnalysisFactory factory;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiSlice slice = analysis.getSlice(sliceId);

    if (slice == null)
      throw new RuntimeException("Unknown slice: " + sliceId);

    if (name == null || "".equals(name))
      throw new RuntimeException("Qube name empty.");

    UiQube qube = factory.createQube(UUID.randomUUID().toString(), name, sliceId);
    analysis.getQubes().add(qube);

    return () -> resultHandler.sendData(new QubeJsonResult(qube, analysis.getVersion()));
  }

}
