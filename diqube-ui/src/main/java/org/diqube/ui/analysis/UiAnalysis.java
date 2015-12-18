/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.ui.analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiAnalysis implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty
  @NotNull
  public String id;

  @JsonProperty
  @NotNull
  public String user;

  @JsonProperty
  @NotNull
  public long version;

  @JsonProperty
  @NotNull
  public String table;

  @JsonProperty
  @NotNull
  public String name;

  @JsonProperty
  @NotNull
  @Valid
  public List<UiQube> qubes = new ArrayList<>();

  @JsonProperty
  @NotNull
  @Valid
  public List<UiSlice> slices = new ArrayList<>();

  // for tests only
  public UiAnalysis() {

  }

  /* package */ UiAnalysis(String id, String name, String table, String user, long version) {
    this.id = id;
    this.name = name;
    this.table = table;
    this.user = user;
    this.version = version;
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

  public UiQube getQube(String id) {
    Optional<UiQube> resQube = qubes.stream().filter(qube -> qube.getId().equals(id)).findAny();

    if (!resQube.isPresent())
      return null;

    return resQube.get();
  }

  public List<UiSlice> getSlices() {
    return slices;
  }

  public UiSlice getSlice(String id) {
    Optional<UiSlice> resSlice = slices.stream().filter(slice -> slice.getId().equals(id)).findAny();

    if (!resSlice.isPresent())
      return null;

    return resSlice.get();
  }

  public String getName() {
    return name;
  }

  public String getUser() {
    return user;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setName(String name) {
    this.name = name;
  }
}
