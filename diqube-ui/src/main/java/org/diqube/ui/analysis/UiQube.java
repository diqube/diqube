package org.diqube.ui.analysis;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiQube {
  @JsonProperty
  public String name;

  @JsonProperty
  public String sliceName;

  @JsonProperty
  public List<UiQuery> queries;

  /* package */ UiQube(String name, String sliceName) {
    this.name = name;
    this.sliceName = sliceName;
  }
}
