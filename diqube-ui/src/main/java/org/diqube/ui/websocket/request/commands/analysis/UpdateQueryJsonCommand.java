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
import org.diqube.ui.analysis.QueryBuilder;
import org.diqube.ui.analysis.QueryBuilder.QueryBuilderException;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.result.analysis.QueryJsonResult;
import org.springframework.util.SerializationUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Updates a queries settings.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link QueryJsonResult} the updated query.
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = UpdateQueryJsonCommand.NAME)
public class UpdateQueryJsonCommand extends AbstractAnalysisAdjustingJsonCommand {

  @TypeScriptProperty
  public static final String NAME = "updateQuery";

  @JsonProperty
  @TypeScriptProperty
  public String qubeId;

  @JsonProperty
  @TypeScriptProperty
  public UiQuery newQuery;

  @Override
  protected Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler) {
    UiQube qube = analysis.getQube(qubeId);
    if (qube == null)
      throw new RuntimeException("Unknown qube: " + qubeId);

    UiQuery query = qube.getQuery(newQuery.getId());
    if (query == null)
      throw new RuntimeException("Unknwon query: " + newQuery.getId());

    if (newQuery.getName() == null || "".equals(newQuery.getName()))
      throw new RuntimeException("Name empty.");

    // validate query!
    try {
      UiQuery queryClone = (UiQuery) SerializationUtils.deserialize(SerializationUtils.serialize(query));
      queryClone.setDiql(newQuery.getDiql());
      queryClone.setName(newQuery.getName());
      queryClone.setDisplayType(newQuery.getDisplayType());

      new QueryBuilder().withAnalysis(analysis).withQuery(queryClone).withSlice(analysis.getSlice(qube.getSliceId()))
          .build();
    } catch (QueryBuilderException e) {
      throw new RuntimeException(e.getMessage());
    }

    query.setDiql(newQuery.getDiql());
    query.setName(newQuery.getName());
    query.setDisplayType(newQuery.getDisplayType());

    return () -> resultHandler.sendData(new QueryJsonResult(query,
        // Analysis version is updated by the time this Runnable is called.
        analysis.getVersion()));
  }

}
