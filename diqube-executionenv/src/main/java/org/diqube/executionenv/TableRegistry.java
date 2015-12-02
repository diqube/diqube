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
package org.diqube.executionenv;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.listeners.TableLoadListener;
import org.diqube.listeners.providers.LoadedTablesProvider;

/**
 * All {@link Table} objects that are available on the current cluster node are registered here.
 * 
 * <p>
 * Note that this does <b>NOT</b> include {@link FlattenedTable}s, as they are managed in
 * {@link FlattenedTableInstanceManager}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class TableRegistry implements LoadedTablesProvider {
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
      tableLoadListeners.forEach(l -> l.tableLoaded(name));
  }

  public synchronized void removeTable(String name) {
    tables.remove(name);

    tableLoadListeners.forEach(l -> l.tableUnloaded(name));
  }

  public synchronized Collection<String> getAllTableNames() {
    return new ArrayList<>(tables.keySet());
  }

  @Override
  public Collection<String> getNamesOfLoadedTables() {
    return new ArrayList<>(tables.keySet());
  }
}
