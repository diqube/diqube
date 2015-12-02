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
package org.diqube.connection;

import org.diqube.remote.base.thrift.RNodeAddress;

/**
 * Listener that is informed as soon as someone found that a specific cluster node died or is alive.
 * 
 * <p>
 * THis listener publicizes detailed information, that means it might publicize the same information multiple times.
 * Implementations should de-duplicate if needed.
 *
 * @author Bastian Gloeckle
 */
public interface ClusterNodeStatusDetailListener {
  /**
   * A specific node in the cluster died.
   */
  public void nodeDied(RNodeAddress nodeAddr);

  /**
   * A specific node in the cluster is alive.
   */
  public void nodeAlive(RNodeAddress nodeAddr) throws InterruptedException;
}
