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

import org.apache.thrift.TException;
import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.diql.DiqlParseUtil;
import org.diqube.diql.ParseException;
import org.diqube.diql.antlr.DiqlParser.DiqlStmtContext;
import org.diqube.diql.request.ExecutionRequest;
import org.diqube.diql.visitors.SelectStmtVisitor;
import org.diqube.name.FunctionBasedColumnNameBuilderFactory;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.db.UiDatabase.StoreException;
import org.diqube.ui.db.UiDbProvider;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.AnalysisJsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Create a new {@link UiAnalysis}.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisJsonResult}
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateAnalysisJsonCommand.NAME)
public class CreateAnalysisJsonCommand implements JsonCommand {
  private static final Logger logger = LoggerFactory.getLogger(CreateAnalysisJsonCommand.class);

  @TypeScriptProperty
  public static final String NAME = "createAnalysis";

  @JsonProperty
  @TypeScriptProperty
  public String table;

  @JsonProperty
  @TypeScriptProperty
  public String name;

  @JsonIgnore
  @Inject
  private AnalysisFactory factory;

  @JsonIgnore
  @Inject
  private UiDbProvider uiDbProvider;

  @JsonIgnore
  @Inject
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;

  @JsonIgnore
  @Inject
  private FunctionBasedColumnNameBuilderFactory functionBasedColumnNameBuilderFactory;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    if (name == null || name.trim().equals(""))
      throw new RuntimeException("Name required.");

    if (table == null || table.trim().equals(""))
      throw new RuntimeException("Table required.");

    UiAnalysis res =
        factory.createAnalysis(UUID.randomUUID().toString(), name, table, ticket.getClaim().getUsername(), 0L);

    try {
      uiDbProvider.getDb().storeAnalysisVersion(res);
    } catch (StoreException e1) {
      logger.error("Could not store new analysis", e1);
      throw new RuntimeException("Could not store new analysis.", e1);
    }

    try {
      // check if "table" is a flattened table and if, ask the cluster to start flattening right away.
      DiqlStmtContext stmtCtx = DiqlParseUtil.parseWithAntlr("select a from " + table);
      ExecutionRequest executionRequest =
          stmtCtx.accept(new SelectStmtVisitor(repeatedColumnNameGenerator, functionBasedColumnNameBuilderFactory));

      if (executionRequest.getFromRequest().isFlattened()) {
        String origTableName = executionRequest.getFromRequest().getTable();
        String flattenBy = executionRequest.getFromRequest().getFlattenByField();
        try {
          clusterInteraction.getFlattenPreparationService().prepareForQueriesOnFlattenedTable(ticket, origTableName,
              flattenBy);
        } catch (TException e) {
          logger.warn("Could not prepare flattening of '{}' by '{}' for new analysis {}", origTableName, flattenBy,
              res.getId());
        }
      }
    } catch (ParseException e) {
      // swallow.
    }

    resultHandler.sendData(new AnalysisJsonResult(res));
  }

}
