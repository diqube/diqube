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
package org.diqube.ui.websocket.json.result;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The envelope of data that the client expects when receiving results.
 *
 * @author Bastian Gloeckle
 */
public class JsonResultEnvelope {
  /** Provide additional result data. */
  public static final String STATUS_DATA = "data";
  /** Provide details of an exception */
  public static final String STATUS_EXCEPTION = "exception";
  /** Inform the client that the execution of the command has completed. */
  public static final String STATUS_DONE = "done";

  /**
   * Property name in the resulting JSON (when serializing) that will contain the data sent along with the envelope.
   * Note that this is not needed in case we send a {@link #STATUS_DONE}.
   */
  public static final String PROPERTY_DATA = "data";

  @JsonProperty
  public String requestId;

  @JsonProperty
  public String status;

  @JsonProperty
  public String dataType;

  /* package */ JsonResultEnvelope(String requestId, String status, String dataType) {
    this.requestId = requestId;
    this.status = status;
    this.dataType = dataType;
  }
}
