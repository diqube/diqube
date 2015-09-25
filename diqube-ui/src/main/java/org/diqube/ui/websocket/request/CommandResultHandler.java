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
package org.diqube.ui.websocket.request;

import org.diqube.ui.websocket.request.commands.JsonCommand;
import org.diqube.ui.websocket.result.JsonResult;

/**
 * Sends results of a {@link JsonCommand} to the client that requested it.
 *
 * @author Bastian Gloeckle
 */
public interface CommandResultHandler {
  /**
   * Send data to the client.
   * 
   * @param data
   *          The data to be sent.
   */
  public void sendData(JsonResult data);

  /**
   * Inform the client that the execution of the command has completed.
   */
  public void sendDone();

  /**
   * Send information to the client that there was an exception while executing the command.
   */
  public void sendException(Throwable t);
}
