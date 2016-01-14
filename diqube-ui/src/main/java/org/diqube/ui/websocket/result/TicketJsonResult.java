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

import org.diqube.build.mojo.TypeScriptProperty;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ticket.TicketUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;

/**
 * Payload containing a login {@link Ticket}.
 *
 * @author Bastian Gloeckle
 */
@JsonResultDataType(TicketJsonResult.TYPE)
public class TicketJsonResult implements JsonResult {
  @TypeScriptProperty
  public static final String TYPE = "ticket";

  /** Base64 serialized ticket */
  @JsonProperty
  @TypeScriptProperty
  public String ticket;

  /** username in the ticket, for convenience */
  @JsonProperty
  @TypeScriptProperty
  public String username;

  public TicketJsonResult() {
  }

  public TicketJsonResult(Ticket ticket) {
    this.ticket = BaseEncoding.base64().encode(TicketUtil.serialize(ticket));
    this.username = ticket.getClaim().getUsername();
  }
}
