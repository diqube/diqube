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

import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.QubeJsonResult;

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
public class UpdateQubeJsonCommand extends AbstractAnalysisAdjustingJsonCommand {

  public static final String NAME = "updateQube";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String qubeName;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String sliceId;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Qube unknwon: " + qubeId);

    if (qubeName == null || "".equals(qubeName))
      throw new RuntimeException("Qube name empty.");

    if (sliceId == null || "".equals(sliceId))
      throw new RuntimeException("SliceId empty.");

    qube.setName(qubeName);
    qube.setSliceId(sliceId);

    return () -> resultHandler.sendData(new QubeJsonResult(qube, analysis.getVersion()));
  }

}
