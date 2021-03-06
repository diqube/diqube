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
import java.util.List;

import org.diqube.build.mojo.TypeScriptProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class UiSliceDisjunction implements Serializable {
  private static final long serialVersionUID = 1L;

  @JsonProperty
  @TypeScriptProperty
  public String fieldName;

  @JsonProperty
  @TypeScriptProperty
  public List<String> disjunctionValues;

  // for tests only
  public UiSliceDisjunction() {

  }

  /* package */ UiSliceDisjunction(String fieldName, List<String> disjunctionValues) {
    this.fieldName = fieldName;
    this.disjunctionValues = disjunctionValues;
  }

  public String getFieldName() {
    return fieldName;
  }

  public List<String> getDisjunctionValues() {
    return disjunctionValues;
  }
}
