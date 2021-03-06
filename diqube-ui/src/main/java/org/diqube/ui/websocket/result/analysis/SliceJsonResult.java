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
package org.diqube.ui.websocket.result.analysis;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.UiSlice;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result containing a slice of an analysis.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(SliceJsonResult.TYPE)
public class SliceJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "slice";

  @JsonProperty
  @TypeScriptProperty
  private UiSlice slice;

  @JsonProperty
  @TypeScriptProperty
  private long analysisVersion;

  // for tests only
  public SliceJsonResult() {

  }

  public SliceJsonResult(UiSlice slice, long analysisVersion) {
    this.slice = slice;
    this.analysisVersion = analysisVersion;
  }
}
