package org.diqube.ui.websocket.request.commands.analysis;

import javax.inject.Inject;

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.AnalysisJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Returns a specific analysis.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link AnalysisJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = AnalysisJsonCommand.NAME)
public class AnalysisJsonCommand implements JsonCommand {

  public static final String NAME = "analysis";

  @JsonProperty
  public String analysisId;

  @JsonIgnore
  @Inject
  public AnalysisRegistry registry;

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);
    if (analysis == null)
      throw new RuntimeException("Analysis not found: " + analysisId);

    resultHandler.sendData(new AnalysisJsonResult(analysis));
  }

}
