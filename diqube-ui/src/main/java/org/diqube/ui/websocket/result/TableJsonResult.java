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

import org.diqube.build.mojo.TypeScriptProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple {@link JsonResult} containing information about results of a query.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(TableJsonResult.TYPE)
public class TableJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "table";

  /**
   * The final names of the columns that were selected (= output of the executed plan).
   */
  @JsonProperty
  @TypeScriptProperty(collectionType = String.class)
  public List<String> columnNames;

  /**
   * Strings of the select statement that lead to the selection of the {@link #columnNames}. Index corresponds to
   * {@link #columnNames}.
   */
  @JsonProperty
  @TypeScriptProperty(collectionType = String.class)
  public List<String> columnRequests;

  @JsonProperty
  @TypeScriptProperty(collectionType = List.class, collectionDetailType = List.class)
  public List<List<Object>> rows;

  @JsonProperty
  @TypeScriptProperty(collectionType = Short.class)
  public short percentComplete;

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public void setRows(List<List<Object>> rows) {
    this.rows = rows;
  }

  public void setPercentComplete(short percentComplete) {
    this.percentComplete = percentComplete;
  }

  public List<String> getColumnRequests() {
    return columnRequests;
  }

  public void setColumnRequests(List<String> columnRequests) {
    this.columnRequests = columnRequests;
  }

}
