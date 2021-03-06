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
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.QueryJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Creates a {@link UiQuery} of a {@link UiQube}.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QueryJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateQueryJsonCommand.NAME)
public class CreateQueryJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "createQuery";

  @JsonProperty
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @TypeScriptProperty
  public String name;

  @JsonProperty
  @TypeScriptProperty
  public String diql;

  @JsonIgnore
  @Inject
  private AnalysisFactory factory;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);

    if (qube == null)
      throw new RuntimeException("Qube not found: " + qubeId);

    UiQuery query = factory.createQuery(UUID.randomUUID().toString(), name, diql, UiQuery.DISPLAY_TYPE_TABLE);

    qube.getQueries().add(query);

    return () -> resultHandler.sendData(new QueryJsonResult(query,
        // Analysis version is updated by the time this Runnable is called.
        analysis.getVersion()));
  }

}
