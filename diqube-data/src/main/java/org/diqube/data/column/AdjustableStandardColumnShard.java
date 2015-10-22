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
package org.diqube.data.column;

/**
 * A {@link StandardColumnShard} of which some properties are adjustable - this is needed for example after
 * deserializing a col shard, if it should adhere to other circumstances.
 *
 * @author Bastian Gloeckle
 */
public interface AdjustableStandardColumnShard extends StandardColumnShard {
  /**
   * Adjusts the values in the column shard and all {@link ColumnPage}s, so that the first row has the given row ID.
   * 
   * This needs to be called before the column is reachable from the TableRegistry.
   * 
   * @throws UnsupportedOperationException
   *           In case the row ID cannot be adjusted.
   */
  public void adjustToFirstRowId(long firstRowId) throws UnsupportedOperationException;
}
