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
package org.diqube.execution.consumers;

import java.util.Set;

import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.GenericConsumer.IdentifyingConsumerClass;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;

/**
 * An {@link OverwritingConsumer} that is called as soon as a new version of a specific column is built. This might
 * happen multiple times for each column.
 * 
 * <p>
 * Compare to {@link ColumnBuiltConsumer}. The {@link ColumnVersionBuiltConsumer} might be used on the query master node
 * and is therefore usually available in each {@link ExecutablePlanStep} where a {@link ColumnBuiltConsumer} is
 * available, too.
 * 
 * <p>
 * Implementations should always listen on the {@link ColumnBuiltConsumer}, too and not only on the
 * {@link ColumnVersionBuiltConsumer}, as the results of {@link ColumnBuiltConsumer} are the "final" ones.
 *
 * @author Bastian Gloeckle
 */
@IdentifyingConsumerClass(ColumnVersionBuiltConsumer.class)
public interface ColumnVersionBuiltConsumer extends OverwritingConsumer {
  /**
   * A new version of a column was built and is available in the provided {@link ExecutionEnvironment}.
   * 
   * @param env
   *          The intermediate {@link ExecutionEnvironment} that contains the built columns. That
   *          {@link ExecutionEnvironment} has been built by a {@link ColumnVersionManager}.
   * @param colName
   *          The name of the column of which a new version is available.
   * @param adjustedRowIds
   *          Those RowIds in the column whose values have changed in this {@link ExecutionEnvironment} compared to the
   *          previous intermediary {@link ExecutionEnvironment}.
   */
  public void columnVersionBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds);
}
