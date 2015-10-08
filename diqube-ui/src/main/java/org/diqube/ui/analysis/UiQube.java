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
public class UiQube implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty
  @NotNull
  public String id;

  @JsonProperty
  @NotNull
  public String name;

  @JsonProperty
  @NotNull
  public String sliceId;

  @JsonProperty
  @NotNull
  @Valid
  public List<UiQuery> queries = new ArrayList<>();

  // for tests only
  public UiQube() {

  }

  /* package */ UiQube(String id, String name, String sliceId) {
    this.id = id;
    this.name = name;
    this.sliceId = sliceId;
  }

  public String getName() {
    return name;
  }

  public String getSliceId() {
    return sliceId;
  }

  public List<UiQuery> getQueries() {
    return queries;
  }

  public UiQuery getQuery(String id) {
    Optional<UiQuery> resQuery = queries.stream().filter(query -> query.getId().equals(id)).findAny();

    if (!resQuery.isPresent())
      return null;

    return resQuery.get();
  }

  public String getId() {
    return id;
  }
}
