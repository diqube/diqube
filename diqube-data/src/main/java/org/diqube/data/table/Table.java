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
package org.diqube.data.table;

import java.util.Collection;

import org.diqube.data.column.ColumnShard;

/**
 * A {@link Table} is the basic container of any data.
 * 
 * <p>
 * From a logical point of view, a Table is made up of columns and rows, whereas each row can hold an arbitrary complex
 * object whose values are split into separate columns ({@link ColumnShard}) on import time.
 * 
 * <p>
 * Technically, a table consists of {@link TableShard}s of which one or multiple might be available on the current
 * machine, and others being resident on other cluster machines. Each {@link TableShard} contains the data of a subset
 * of the tables rows. Each TableShard then contains a set of {@link ColumnShard}s, which in turn contain the actual
 * data. Note that each TableShard might contain a different set of columns, as each TableShard only materializes those
 * Columns, that it actually has data for.
 *
 * @author Bastian Gloeckle
 */
public interface Table {
  /**
   * @return Name of the table
   */
  public String getName();

  /**
   * @return The {@link TableShard}s of the table. The returned collection is unmodifiable and does not contain the
   *         elements in a specific order.
   */
  public Collection<TableShard> getShards();

}
