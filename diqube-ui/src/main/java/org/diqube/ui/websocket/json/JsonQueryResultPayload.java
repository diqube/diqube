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
package org.diqube.ui.websocket.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author Bastian Gloeckle
 */
public class JsonQueryResultPayload implements JsonPayload {
  public static final String PAYLOAD_TYPE = "result";

  @JsonProperty
  public List<String> columnNames;

  @JsonProperty
  public List<List<Object>> rows;

  @JsonProperty
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

  @Override
  public String getPayloadType() {
    return PAYLOAD_TYPE;
  }
}
