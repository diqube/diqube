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

import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiQuery implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String DISPLAY_TYPE_TABLE = "table";
  public static final String DISPLAY_TYPE_BARCHART = "barchart";

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String id;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String name;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String diql;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public String displayType;

  /** for tests only */
  public UiQuery() {

  }

  /* package */ UiQuery(String id, String name, String diql, String displayType) {
    this.id = id;
    this.name = name;
    this.diql = diql;
    this.displayType = displayType;
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

  public String getDisplayType() {
    return displayType;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setDiql(String diql) {
    this.diql = diql;
  }

  public void setDisplayType(String displayType) {
    this.displayType = displayType;
  }
}
