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
package org.diqube.listeners;

import org.diqube.context.AutoInstatiate;

/**
 * Listener that gets informed when tables are un-/loaded.
 * 
 * All implementing classes need to have a bean inside the context (= need to have the {@link AutoInstatiate}
 * annotation).
 *
 * @author Bastian Gloeckle
 */
public interface TableLoadListener {
  /**
   * A new Table has been loaded and is available in the TableRegistry.
   * 
   * @param newTableName
   *          Name of the table the new shard belongs to.
   */
  public void tableLoaded(String newTableName);

  /**
   * A Table has been unloaded and is no longer available in the TableRegistry.
   * 
   * @param tableName
   *          Name of the table the shard belonged to.
   */
  public void tableUnloaded(String tableName);
}
