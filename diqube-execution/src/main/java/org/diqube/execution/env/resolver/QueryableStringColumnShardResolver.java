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
package org.diqube.execution.env.resolver;

import org.diqube.data.table.TableShard;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;

/**
 * Can resolve {@link QueryableStringColumnShard}s in a meaningful way.
 *
 * @author Bastian Gloeckle
 */
public interface QueryableStringColumnShardResolver {

  /**
   * Returns a {@link QueryableStringColumnShard} for a specific column.
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link QueryableStringColumnShard} for the column with the given name or <code>null</code> if it does not
   *         exist.
   */
  public QueryableStringColumnShard getStringColumnShard(String name);
}
