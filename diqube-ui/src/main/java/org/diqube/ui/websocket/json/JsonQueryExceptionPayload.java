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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple {@link JsonPayload} containing information about an exception that occured while executing a query.
 *
 * @author Bastian Gloeckle
 */
public class JsonQueryExceptionPayload implements JsonPayload {
  public static final String PAYLOAD_TYPE = "exception";

  @JsonProperty
  public String text;

  @Override
  public String getPayloadType() {
    return PAYLOAD_TYPE;
  }

  public void setText(String text) {
    this.text = text;
  }
}
