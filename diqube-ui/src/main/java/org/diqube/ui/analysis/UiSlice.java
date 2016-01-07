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

import org.diqube.build.mojo.TypeScriptProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiSlice implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty
  @TypeScriptProperty
  public String id;

  @JsonProperty
  @TypeScriptProperty
  public String name;

  @JsonProperty
  @TypeScriptProperty
  public List<UiSliceDisjunction> sliceDisjunctions = new ArrayList<>();

  @JsonProperty
  public String manualConjunction;

  // for tests only
  public UiSlice() {

  }

  /* package */ UiSlice(String id, String name, List<UiSliceDisjunction> sliceDisjunctions) {
    this.id = id;
    this.name = name;
    this.sliceDisjunctions = sliceDisjunctions;
  }

  public String getName() {
    return name;
  }

  public List<UiSliceDisjunction> getSliceDisjunctions() {
    return sliceDisjunctions;
  }

  public String getId() {
    return id;
  }

  public String getManualConjunction() {
    return manualConjunction;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setManualConjunction(String manualConjunction) {
    this.manualConjunction = manualConjunction;
  }

  public void setSliceDisjunctions(List<UiSliceDisjunction> sliceDisjunctions) {
    this.sliceDisjunctions = sliceDisjunctions;
  }
}
