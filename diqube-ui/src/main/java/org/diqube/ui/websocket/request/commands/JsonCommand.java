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
package org.diqube.ui.websocket.request.commands;

import org.diqube.thrift.base.thrift.AuthenticationException;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.ui.websocket.request.CommandClusterInteraction;
import org.diqube.ui.websocket.request.CommandResultHandler;
import org.diqube.ui.websocket.request.JsonRequestDeserializer;

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
   * @param ticket
   *          The ticket identifying the user. <code>null</code> if user is not logged in. Note that the validity of the
   *          Ticket (if one is available) was verified already before the ticket is passed to any JsonCommand, but
   *          anyhow, the ticket might be rejected. If a command needs the ticket, it should check if it is != null.
   * @param resultHandler
   *          Can be called to send any results to the client.
   * @param clusterInteraction
   *          access to the diqube-server cluster for the command.
   * 
   * @throws RuntimeException
   *           is thrown if anything goes wrong.
   * @throws AuthenticationException
   *           is thrown if the provided ticket is not valid/was rejected by diqube-server.
   */
  public abstract void execute(Ticket ticket, CommandResultHandler resultHandler,
      CommandClusterInteraction clusterInteraction) throws RuntimeException, AuthenticationException;

}
