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
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.AnalysisVersionJsonResult;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Remove a query.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisVersionJsonResult}.
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = RemoveQueryJsonCommand.NAME)
public class RemoveQueryJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "removeQuery";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String queryId;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Unknown qube: " + qubeId);

    UiQuery query = qube.getQuery(queryId);
    if (query == null)
      throw new RuntimeException("Unknwon query: " + queryId);

    qube.getQueries().remove(query);

    return () -> resultHandler.sendData(new AnalysisVersionJsonResult(analysis.getVersion()));
  }

}
