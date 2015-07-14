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

import java.util.Collection;
import java.util.NavigableSet;
import java.util.Set;

import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;

/**
 * Helper class providing methods for handling row IDs when a {@link ColumnVersionBuiltConsumer} is in place.
 *
 * @author Bastian Gloeckle
 */
public class ColumnVersionBuiltHelper {
  /**
   * Finds those row IDs out of a set of row IDs whose values are all available in an {@link ExecutionEnvironment} and
   * manages a set of row IDs that could not be processed yet.
   * 
   * @param env
   *          The {@link ExecutionEnvironment} that is queried for the rows that are available in all columns. // -----
   *          TODO #8 support subset of cols
   * @param activeRowIds
   *          The row IDs that should be worked on now as provided by any other input {@link GenericConsumer}. This set
   *          will be adjusted accordingly. After this method returns, the activeRowIds will contain only row IDs that
   *          are available in {@link ExecutionEnvironment}. It might contain any additional row IDs of the
   *          notYetProcessedRowIds parameter object, as those row IDs were not processed yet, but it might be possible
   *          to process them now as the corresponding rows became available in the {@link ExecutionEnvironment}. Those
   *          row IDs that are added to activeRowIds from notYetProcessedRowIds are removed from the latter.
   * @param notYetProcessedRowIds
   *          A set containing those row IDs that have not yet been processed. See details above.
   * @return The maximum row ID for which all columns contain values.
   */
  public long publishActiveRowIds(ExecutionEnvironment env, NavigableSet<Long> activeRowIds,
      NavigableSet<Long> notYetProcessedRowIds) {
    long maxRowId;
    Collection<ColumnShard> cols = env.getAllColumnShards().values();
    if (cols.stream().anyMatch(col -> col instanceof StandardColumnShard)) {
      maxRowId = cols.stream().filter(col -> col instanceof StandardColumnShard)
          .mapToLong(
              column -> column.getFirstRowId() + ((StandardColumnShard) column).getNumberOfRowsInColumnShard() - 1)
          . //
          min().getAsLong();
    } else
      // only ConstantColumnShards
      maxRowId = cols.iterator().next().getFirstRowId();

    Set<Long> activeRowIdsNotAvailable = activeRowIds.tailSet(maxRowId, false);
    if (!activeRowIdsNotAvailable.isEmpty()) {
      notYetProcessedRowIds.addAll(activeRowIdsNotAvailable);
      activeRowIdsNotAvailable.clear();
    }
    Set<Long> notYetProcessedAvailable = notYetProcessedRowIds.headSet(maxRowId, true);
    if (!notYetProcessedAvailable.isEmpty()) {
      activeRowIds.addAll(notYetProcessedAvailable);
      notYetProcessedAvailable.clear();
    }
    return maxRowId;
  }
}
