package org.diqube.ui.analysis;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiSliceDisjunction {
  @JsonProperty
  public String fieldName;

  @JsonProperty
  public List<String> disjunctionValues;

  /* package */ UiSliceDisjunction(String fieldName, List<String> disjunctionValues) {
    this.fieldName = fieldName;
    this.disjunctionValues = disjunctionValues;
  }
}
