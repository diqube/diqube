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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.analysis.util.QueryInfoJsonResultBuilderFactory;
import org.diqube.ui.websocket.result.analysis.QueryInfoJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Finds out additional (meta)information about a specific query.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QueryInfoJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = QueryInfoJsonCommand.NAME)
public class QueryInfoJsonCommand extends AbstractAnalysisAdjustingJsonCommand {
  @TypeScriptProperty
  public static final String NAME = "getQueryInfo";

  @JsonProperty
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @TypeScriptProperty
  public String queryId;

  @JsonIgnore
  @Inject
  private QueryInfoJsonResultBuilderFactory queryInfoJsonResultBuilderFactory;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);

    if (qube == null)
      throw new RuntimeException("Qube not found: " + qubeId);

    UiQuery query = qube.getQuery(queryId);
    if (query == null)
      throw new RuntimeException("Query not found: " + queryId);

    UiSlice slice = analysis.getSlice(qube.getSliceId());
    if (slice == null)
      throw new RuntimeException("Slice not found: " + qube.getSliceId());

    QueryInfoJsonResult res = queryInfoJsonResultBuilderFactory.createBuilder().withQueryId(queryId)
        .withAnalysis(analysis).withQuery(query).withSlice(slice).build();

    return () -> resultHandler.sendData(res);
  }

}
