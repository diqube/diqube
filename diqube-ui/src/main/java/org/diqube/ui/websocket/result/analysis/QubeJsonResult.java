package org.diqube.ui.websocket.result.analysis;

import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of a single qube.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(QubeJsonResult.DATA_TYPE)
public class QubeJsonResult implements JsonResult {
  public static final String DATA_TYPE = "qube";

  @JsonProperty
  public UiQube qube;

  public QubeJsonResult(UiQube qube) {
    this.qube = qube;
  }
}
