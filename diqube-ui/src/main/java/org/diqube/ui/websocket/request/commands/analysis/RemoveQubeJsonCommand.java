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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.AnalysisVersionJsonResult;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Removes a qube.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisVersionJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = RemoveQubeJsonCommand.NAME)
public class RemoveQubeJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "removeQube";

  @JsonProperty
  @TypeScriptProperty
  public String qubeId;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Unknown qube: " + qubeId);

    analysis.getQubes().remove(qube);

    return () -> resultHandler.sendData(new AnalysisVersionJsonResult(
        // Analysis version is updated by the time this Runnable is called.
        analysis.getVersion()));
  }

}
