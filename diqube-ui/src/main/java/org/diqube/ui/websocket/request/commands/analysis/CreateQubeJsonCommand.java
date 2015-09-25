package org.diqube.ui.websocket.request.commands.analysis;

import java.util.ArrayList;

import javax.inject.Inject;

import org.diqube.ui.AnalysisRegistry;
import org.diqube.ui.analysis.AnalysisFactory;
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.commands.CommandInformation;
import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.analysis.QubeJsonResult;
import org.diqube.ui.websocket.result.analysis.SliceJsonResult;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Creates a new qube inside an analysis. If there are no slices yet in the analysis or the given slice does not exist,
 * a new slice is added, too.
 *
 * <p>
 * Sends following results:
 * <ul>
 * <li>{@link SliceJsonResult} (optional; if sent, then this is sent first).
 * <li>{@link QubeJsonResult}
 * </ul>
 * 
 * @author Bastian Gloeckle
 */
@CommandInformation(name = CreateQubeJsonCommand.NAME)
public class CreateQubeJsonCommand implements JsonCommand {

  public static final String NAME = "createQube";

  private static final String DEFAULT_SLICE_NAME = "Default";
  private static final String DEFAULT_QUBE_NAME = "Default";

  @JsonProperty
  private String analysisId;

  @JsonProperty(required = false)
  private String sliceName;

  @JsonProperty(required = false)
  private String name;

  @Inject
  @JsonIgnore
  private AnalysisRegistry registry;

  @Inject
  @JsonIgnore
  private AnalysisFactory factory;

  @Override
  public void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException {
    UiAnalysis analysis = registry.getAnalysis(analysisId);

    if (sliceName == null)
      sliceName = DEFAULT_SLICE_NAME;

    if (analysis == null)
      throw new RuntimeException("Unknown analysis: " + analysisId);

    UiSlice slice = analysis.getSlice(sliceName);
    if (analysis.getSlices().isEmpty() || slice == null) {
      slice = factory.createSlice(sliceName, new ArrayList<>());
      analysis.getSlices().add(slice);
      resultHandler.sendData(new SliceJsonResult(slice));
    }

    if (name == null)
      name = DEFAULT_QUBE_NAME;

    UiQube qube = factory.createQube(name, sliceName);
    analysis.getQubes().add(qube);
    resultHandler.sendData(new QubeJsonResult(qube));
  }

}
