package org.diqube.ui.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiAnalysis {
  @JsonProperty
  public String id;

  @JsonProperty
  public String table;

  @JsonProperty
  public String name;

  @JsonProperty
  public List<UiQube> qubes = new ArrayList<>();

  @JsonProperty
  public List<UiSlice> slices = new ArrayList<>();

  /* package */ UiAnalysis(String id, String name, String table) {
    this.id = id;
    this.name = name;
    this.table = table;
  }

  public String getId() {
    return id;
  }

  public String getTable() {
    return table;
  }

  public List<UiQube> getQubes() {
    return qubes;
  }

  public List<UiSlice> getSlices() {
    return slices;
  }

  public UiSlice getSlice(String name) {
    Optional<UiSlice> resSlice = slices.stream().filter(slice -> slice.getName().equals(name)).findAny();

    if (!resSlice.isPresent())
      return null;

    return resSlice.get();
  }

  public String getName() {
    return name;
  }
}
