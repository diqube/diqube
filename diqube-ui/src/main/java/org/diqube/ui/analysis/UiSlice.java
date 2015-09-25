package org.diqube.ui.analysis;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiSlice {
  @JsonProperty
  public String name;

  @JsonProperty
  public List<UiSliceDisjunction> sliceDisjunctions;

  /* package */ UiSlice(String name, List<UiSliceDisjunction> sliceDisjunctions) {
    this.name = name;
    this.sliceDisjunctions = sliceDisjunctions;
  }

  public String getName() {
    return name;
  }

  public List<UiSliceDisjunction> getSliceDisjunctions() {
    return sliceDisjunctions;
  }
}
