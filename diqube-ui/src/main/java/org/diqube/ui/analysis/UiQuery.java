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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiQuery {
  @JsonProperty
  public String id;

  @JsonProperty
  public String name;

  @JsonProperty
  public String diql;

  /* package */ UiQuery(String id, String name, String diql) {
    this.id = id;
    this.name = name;
    this.diql = diql;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDiql() {
    return diql;
  }
}
