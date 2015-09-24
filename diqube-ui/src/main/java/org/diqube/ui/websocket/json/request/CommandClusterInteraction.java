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
package org.diqube.ui.websocket.json.request;

import org.diqube.remote.query.thrift.QueryResultService;

/**
 * Enables a command to interact with the diqube-server cluster.
 * 
 * An instance of this interface is specific to a session/requestId.
 *
 * @author Bastian Gloeckle
 */
public interface CommandClusterInteraction {
  /**
   * Execute a diql query and provide results to the given result handler.
   */
  public void executeDiqlQuery(String diql, QueryResultService.Iface resultHandler);

  /**
   * Execute the query that was started with
   * {@link #executeDiqlQuery(String, org.diqube.remote.query.thrift.QueryResultService.Iface)}.
   */
  public void cancelQuery();
}
