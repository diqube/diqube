package org.diqube.ui.analysis;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiQuery {
  @JsonProperty
  public String id;

  @JsonProperty
  public String diql;

  /* package */ UiQuery(String id, String diql) {
    this.id = id;
    this.diql = diql;
  }
}
