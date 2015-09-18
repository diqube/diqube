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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.data.Table;
import org.diqube.listeners.TableLoadListener;

/**
 * All {@link Table} objects that are available on the current cluster node are registered here.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableRegistry {
  private Map<String, Table> tables = new HashMap<String, Table>();

  @InjectOptional
  private List<TableLoadListener> tableLoadListeners;

  public synchronized Table getTable(String name) {
    return tables.get(name);
  }

  public synchronized void addTable(String name, Table table) throws IllegalStateException {
    if (tables.containsKey(name))
      throw new IllegalStateException("Table '" + name + "' exists already.");
    tables.put(name, table);

    if (tableLoadListeners != null)
      tableLoadListeners.forEach(l -> l.tableShardLoaded(name));
  }

  public synchronized void removeTable(String name) {
    tables.remove(name);

    tableLoadListeners.forEach(l -> l.tableShardUnloaded(name, false));
  }

  public synchronized Collection<String> getAllTableNames() {
    return new ArrayList<>(tables.keySet());
  }
}
