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
package org.diqube.ui.websocket;

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.diqube.ui.websocket.json.JsonCommand;
import org.diqube.ui.websocket.json.JsonCommandFactory;

/**
 *
 * @author Bastian Gloeckle
 */
@ServerEndpoint("/socket")
public class WebSocketEndpoint {

  private JsonCommandFactory commandFactory = new JsonCommandFactory();

  @OnMessage
  public void onMessage(String msg, Session session) {
    JsonCommand cmd = commandFactory.createCommand(msg, session);
    if (cmd == null)
      throw new RuntimeException("Could not correctly deserialize command!");
    cmd.execute();
  }

  @OnClose
  public void onClose(Session session, CloseReason reason) {
    System.out.println("Received CLOSE (" + session + "): " + reason);
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    System.out.println("Received ERROR (" + session + "): " + throwable.toString());
  }

}
