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

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.AnalysisRefJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Lists all available analysis.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisRefJsonResult} (multiple)
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = ListAllAnalysisJsonCommand.NAME)
public class ListAllAnalysisJsonCommand implements JsonCommand {

  public static final String NAME = "listAllAnalysis";

  @Inject
  @JsonIgnore
  private AnalysisRegistry registry;

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    for (UiAnalysis analysis : registry.getAllAnalysis()) {
      resultHandler.sendData(new AnalysisRefJsonResult(analysis.getName(), analysis.getId()));
    }
  }

}
