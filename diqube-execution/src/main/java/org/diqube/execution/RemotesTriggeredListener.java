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
package org.diqube.execution;

import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;

/**
 * A listener that can be installed on an {@link ExecuteRemotePlanOnShardsStep} in order to be informed about how many
 * remotes that step will trigger.
 *
 * @author Bastian Gloeckle
 */
public interface RemotesTriggeredListener {
  /**
   * Informs about the number of remotes to which the remote execution plan will be distributed to.
   * 
   * This method is called before actually distributing the work.
   */
  public void numberOfRemotesTriggered(int numberOfRemotes);
}
