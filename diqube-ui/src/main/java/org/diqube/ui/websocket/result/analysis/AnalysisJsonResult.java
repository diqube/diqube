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
import org.diqube.ui.analysis.UiAnalysis;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains information about a {@link UiAnalysis}.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(AnalysisJsonResult.TYPE)
public class AnalysisJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "analysis";

  @JsonProperty
  @TypeScriptProperty
  public UiAnalysis analysis;

  // for testing
  public AnalysisJsonResult() {
  }

  public AnalysisJsonResult(UiAnalysis analysis) {
    this.analysis = analysis;
  }
}
