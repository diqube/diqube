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

import org.diqube.data.table.TableShard;

/**
 * A {@link ConstantColumnShard} is a {@link ColumnShard} that has a single value for all rows.
 * 
 * <p>
 * This type of {@link ColumnShard} is not used for columns that are available directly from an {@link TableShard}, but
 * only for temporary columns while executing a query (see ExecutionEnvironment and VersionedExecutionEnvironment). This
 * type of {@link ColumnShard} is used e.g. for columns that are created by a ProjectStep which only projects constant
 * values.
 * 
 * <p>
 * For example if the projection 'add(1, 2)' is executed, there is a corresponding {@link ColumnShard} built and stored
 * to the ExecutionEnvironment that contains the result of that function execution. As the input parameters of the
 * function are constants, we do not need to create a {@link StandardColumnShard} that would have the capability to map
 * each row to a different value, but we only need to store the result of the function once and use it in consecutive
 * ExecutablePlanSteps for all rows.
 *
 * @author Bastian Gloeckle
 */
public interface ConstantColumnShard extends ColumnShard {
  /**
   * @return The actual constant value that should be used for all rows.
   */
  public Object getValue();

  /**
   * @return The Column Shard dictionary ID used in {@link #getColumnShardDictionary()} to map to the value.
   */
  public long getSingleColumnDictId();
}
