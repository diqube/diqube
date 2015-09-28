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
}
