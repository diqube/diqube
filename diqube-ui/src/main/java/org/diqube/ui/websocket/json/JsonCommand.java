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

import javax.websocket.Session;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A command that was sent by the browser encoded in JSON.
 * 
 * The command typically contains some parameters which are deserialized into the fields of a JsonCommand and then the
 * {@link #execute()} method is called to actually execute the logic requested by the browser.
 *
 * @author Bastian Gloeckle
 */
public abstract class JsonCommand implements JsonPayload {
  @JsonIgnore
  protected Session websocketSession;

  protected Session getWebsocketSession() {
    return websocketSession;
  }

  public void setWebsocketSession(Session websocketSession) {
    this.websocketSession = websocketSession;
  }

  /**
   * Execute this command. This can be used after the command has been created and initialized fully by
   * {@link JsonPayloadDeserializer}.
   * 
   * @throws RuntimeException
   *           is thrown if anything goes wrong.
   */
  public abstract void execute() throws RuntimeException;

}
