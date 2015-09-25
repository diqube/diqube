package org.diqube.ui.websocket.result.analysis;

import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains information about a {@link UiAnalysis}.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(AnalysisJsonResult.DATA_TYPE)
public class AnalysisJsonResult implements JsonResult {
  public static final String DATA_TYPE = "analysis";

  @JsonProperty
  public UiAnalysis analysis;

  public AnalysisJsonResult(UiAnalysis analysis) {
    this.analysis = analysis;
  }
}
