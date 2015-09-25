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
