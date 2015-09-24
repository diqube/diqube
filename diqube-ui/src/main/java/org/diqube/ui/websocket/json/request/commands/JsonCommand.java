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
package org.diqube.ui.websocket.json.request.commands;

import org.diqube.ui.websocket.json.request.CommandClusterInteraction;
import org.diqube.ui.websocket.json.request.CommandResultHandler;
import org.diqube.ui.websocket.json.request.JsonRequestDeserializer;

/**
 * A command that is executable and was sent by the browser encoded in JSON.
 *
 * <p>
 * The command typically contains some parameters which are deserialized into the fields of a {@link JsonCommand} by
 * {@link JsonRequestDeserializer} (see there for more information) and then the {@link #execute(CommandResultHandler)}
 * method is called to actually execute the logic requested by the browser.
 * 
 * <p>
 * An instance that wants to be callable by the browser needs to have a {@link CommandInformation} annotation.
 * 
 * @author Bastian Gloeckle
 */
public interface JsonCommand {

  /**
   * Execute this command after this object has been fully initialized.
   * 
   * <p>
   * This method might either work synchronously (= all work has been done after this method returns) or asynchronously.
   * You can switch that using the {@link CommandInformation#synchronous()} flag. Note that synchronous implementations
   * do not need to call {@link CommandResultHandler#sendDone()} when they're done, although asynchronous ones do need
   * to call this.
   * 
   * @param resultHandler
   *          Can be called to send any results to the client.
   * @param clusterInteraction
   *          access to the diqube-server cluster for the command.
   * @throws RuntimeException
   *           is thrown if anything goes wrong.
   */
  public abstract void execute(CommandResultHandler resultHandler, CommandClusterInteraction clusterInteraction)
      throws RuntimeException;

}
