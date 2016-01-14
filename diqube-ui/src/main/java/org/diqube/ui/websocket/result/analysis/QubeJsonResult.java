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
import org.diqube.ui.analysis.UiQube;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of a single qube.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(QubeJsonResult.TYPE)
public class QubeJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "qube";

  @JsonProperty
  @TypeScriptProperty
  public UiQube qube;

  @JsonProperty
  @TypeScriptProperty
  public long analysisVersion;

  // for tests only
  public QubeJsonResult() {

  }

  public QubeJsonResult(UiQube qube, long analysisVersion) {
    this.qube = qube;
    this.analysisVersion = analysisVersion;
  }
}
