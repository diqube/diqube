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
package org.diqube.ui.websocket.result;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.diqube.build.mojo.TypeScriptProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A list of table names.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(TableNameListJsonResult.TYPE)
public class TableNameListJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "tableNameList";

  @JsonProperty
  @NotNull
  @TypeScriptProperty(collectionType = String.class)
  public List<String> tableNames;

  // for test
  public TableNameListJsonResult() {

  }

  public TableNameListJsonResult(List<String> tableNames) {
    this.tableNames = tableNames;
  }
}
