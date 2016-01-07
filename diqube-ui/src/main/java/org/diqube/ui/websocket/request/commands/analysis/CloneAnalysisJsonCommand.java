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
import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
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
import org.springframework.util.SerializationUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Clones an analysis (of any user) to a new analysis of the current user.
 * 
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisJsonResult}
 * </ul>
 *
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CloneAnalysisJsonCommand.NAME)
public class CloneAnalysisJsonCommand implements JsonCommand {
  private static final Logger logger = LoggerFactory.getLogger(CloneAnalysisJsonCommand.class);

  @TypeScriptProperty
  public static final String NAME = "cloneAnalysis";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String analysisId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public long analysisVersion;

  @JsonIgnore
  @Inject
  private AnalysisFactory factory;

  @JsonIgnore
  @Inject
  private UiDbProvider uiDbProvider;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    UiAnalysis originalAnalysis = uiDbProvider.getDb().loadAnalysisVersion(analysisId, analysisVersion);

    if (originalAnalysis == null)
      throw new RuntimeException("Analysis unknown: " + analysisId + " version " + analysisVersion);

    byte[] serialized = SerializationUtils.serialize(originalAnalysis);
    UiAnalysis newAnalysis = (UiAnalysis) SerializationUtils.deserialize(serialized);

    newAnalysis.setId(UUID.randomUUID().toString());
    newAnalysis.setUser(ticket.getClaim().getUsername());
    newAnalysis.setVersion(0L);
    newAnalysis.setName(originalAnalysis.getName() + " [cloned]");

    try {
      uiDbProvider.getDb().storeAnalysisVersion(newAnalysis);
    } catch (StoreException e1) {
      logger.error("Could not store new analysis", e1);
      throw new RuntimeException("Could not store new analysis.", e1);
    }

    resultHandler.sendData(new AnalysisJsonResult(newAnalysis));
  }

}
