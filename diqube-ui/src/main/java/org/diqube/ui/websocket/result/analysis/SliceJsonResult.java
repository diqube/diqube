package org.diqube.ui.websocket.result.analysis;

import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result containing a slice of an analysis.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(SliceJsonResult.DATA_TYPE)
public class SliceJsonResult implements JsonResult {
  public static final String DATA_TYPE = "slice";

  @JsonProperty
  private UiSlice slice;

  public SliceJsonResult(UiSlice slice) {
    this.slice = slice;
  }
}
