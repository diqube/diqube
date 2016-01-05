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

import java.util.Map;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.db.UiDatabase.StoreException;
import org.diqube.ui.db.UiDbProvider;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Abstract JsonCommand for commands that adjust {@link UiAnalysis}.
 * 
 * Implementing classes must have a {@link CommandInformation} annotation.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractAnalysisAdjustingJsonCommand implements JsonCommand {
  private static final Logger logger = LoggerFactory.getLogger(AbstractAnalysisAdjustingJsonCommand.class);

  @JsonIgnore
  @Inject
  private UiDbProvider uiDbProvider;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  protected String analysisId;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  protected long analysisVersion;

  @Override
  public void execute(Ticket ticket, CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException, AuthenticationException {
    if (ticket == null)
      throw new RuntimeException("Not logged in.");

    Map<String, Long> versions = uiDbProvider.getDb().findNewestAnalysisVersionsOfUser(ticket.getClaim().getUsername());

    UiAnalysis analysis = null;
    if (versions.containsKey(analysisId))
      analysis = uiDbProvider.getDb().loadAnalysisVersion(analysisId, analysisVersion);

    if (!versions.containsKey(analysisId) || analysis == null)
      throw new RuntimeException("Unknown analysis or no write access for current user: " + analysisId);

    Runnable sendResult = adjustAnalysis(analysis, resultHandler);

    try {
      // no matter what version of the analysis we changed, the new version number is one higher than the newest one.
      // UI displays a warning in case an old version "overwrites" a new one.
      analysis.setVersion(versions.get(analysisId) + 1);
      uiDbProvider.getDb().storeAnalysisVersion(analysis);
    } catch (StoreException e) {
      logger.error("Could not store new analysis version", e);
      throw new RuntimeException("Could not store new analysis version", e);
    }

    if (sendResult != null)
      sendResult.run();
  }

  /**
   * Implement by subclasses: Adjusts the already loaded analysis object.
   * 
   * @param analysis
   *          Analysis to be adjusted.
   * @param resultHandler
   *          the resultHandler that can be called in the returned {@link Runnable}.
   * @return A runnable that will inform the clients when called. <code>null</code> if nothing needs to be called. When
   *         the runnable is called, {@link UiAnalysis#getVersion()} on the given analysis object can be used to get the
   *         new version of the analysis.
   */
  protected abstract Runnable adjustAnalysis(UiAnalysis analysis, CommandResultHandler resultHandler);

}
