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
import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.analysis.QueryBuilder;
import org.diqube.ui.analysis.QueryBuilder.QueryBuilderException;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.db.UiDbProvider;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.AsyncJsonCommand;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.PlainQueryJsonCommand;
import org.diqube.ui.websocket.result.StatsJsonResult;
import org.diqube.ui.websocket.result.TableJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Executes a {@link UiQuery} of a {@link UiAnalysis}.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link TableJsonResult} (multiple)
 * <li>{@link StatsJsonResult}
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = AnalysisQueryJsonCommand.NAME)
public class AnalysisQueryJsonCommand implements AsyncJsonCommand {

  public static final String NAME = "analysisQuery";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String analysisId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public long analysisVersion;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String queryId;

  @JsonIgnore
  @Inject
  private UiDbProvider uiDbProvider;

  private PlainQueryJsonCommand plainQueryJsonCommand;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    UiAnalysis analysis = uiDbProvider.getDb().loadAnalysisVersion(analysisId, analysisVersion);
    if (analysis == null)
      throw new RuntimeException("Analysis unknown: " + analysisId);

    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Qube unknown: " + qubeId);

    String sliceId = qube.getSliceId();
    UiSlice slice = analysis.getSlice(sliceId);
    if (slice == null)
      throw new RuntimeException("Could not find slice of qube. SliceID: " + sliceId);

    UiQuery query = qube.getQuery(queryId);
    if (query == null)
      throw new RuntimeException("Query unknwon: " + queryId);

    QueryBuilder queryBuilder = new QueryBuilder().withAnalysis(analysis).withSlice(slice).withQuery(query);
    String finalDiql;
    try {
      finalDiql = queryBuilder.build();
    } catch (QueryBuilderException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    plainQueryJsonCommand = new PlainQueryJsonCommand(finalDiql);
    plainQueryJsonCommand.execute(ticket, resultHandler, clusterInteraction);
  }

  @Override
  public void cancel(CommandClusterInteraction clusterInteraction) {
    if (plainQueryJsonCommand != null)
      plainQueryJsonCommand.cancel(clusterInteraction);
  }

}
