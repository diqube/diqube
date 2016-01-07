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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.ui.analysis.UiQuery;
import org.diqube.ui.websocket.result.JsonResult;
import org.diqube.ui.websocket.result.JsonResultDataType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of a single query.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(QueryJsonResult.TYPE)
public class QueryJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "query";

  @JsonProperty
  @NotNull
  @Valid
  @TypeScriptProperty
  public UiQuery query;

  @JsonProperty
  @NotNull
  @TypeScriptProperty
  public long analysisVersion;

  // for tests only
  public QueryJsonResult() {

  }

  public QueryJsonResult(UiQuery query, long analysisVersion) {
    this.query = query;
    this.analysisVersion = analysisVersion;
  }
}
