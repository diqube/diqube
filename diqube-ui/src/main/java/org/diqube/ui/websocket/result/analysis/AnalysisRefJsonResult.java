package org.diqube.ui.websocket.result.analysis;

import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Basic information about an available analysis.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(AnalysisRefJsonResult.DATA_TYPE)
public class AnalysisRefJsonResult implements JsonResult {
  public static final String DATA_TYPE = "analysisRef";

  @JsonProperty
  public String name;

  @JsonProperty
  public String id;

  public AnalysisRefJsonResult(String name, String id) {
    this.name = name;
    this.id = id;
  }
}
