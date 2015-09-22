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
import java.util.stream.Collectors;

import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.querystats.QueryableColumnShard;

/**
 * Helper class providing methods for handling row IDs when a {@link ColumnVersionBuiltConsumer} is in place.
 *
 * @author Bastian Gloeckle
 */
public class ColumnVersionBuiltHelper {
  /**
   * Finds those row IDs out of a set of row IDs whose values are all available in specific columns of an
   * {@link ExecutionEnvironment} and manages a set of row IDs that could not be processed yet.
   * 
   * @param env
   *          The {@link ExecutionEnvironment} that is queried for the rows that are available in the specified columns.
   * @param columns
   *          The columns that should be checked in "env" for the availability of values for the given rowIds.
   * @param activeRowIds
   *          The row IDs that should be worked on now as provided by any other input {@link GenericConsumer}. This set
   *          will be adjusted accordingly. After this method returns, the activeRowIds will contain only row IDs that
   *          are available in {@link ExecutionEnvironment}. It might contain any additional row IDs of the
   *          notYetProcessedRowIds parameter object, as those row IDs were not processed yet, but it might be possible
   *          to process them now as the corresponding rows became available in the {@link ExecutionEnvironment}. Those
   *          row IDs that are added to activeRowIds from notYetProcessedRowIds are removed from the latter.
   * @param notYetProcessedRowIds
   *          A set containing those row IDs that have not yet been processed. See details above.
   * @return The maximum row ID for which all columns contain values or -1 if not all columns are available in the given
   *         env.
   */
  public long publishActiveRowIds(ExecutionEnvironment env, Collection<String> columns, NavigableSet<Long> activeRowIds,
      NavigableSet<Long> notYetProcessedRowIds) {
    long maxRowId;
    Collection<QueryableColumnShard> cols =
        columns.stream().map(colName -> env.getColumnShard(colName)).collect(Collectors.toList());

    if (cols.stream().anyMatch(col -> col == null)) {
      // at least one of the needed columns is not available at all yet.
      notYetProcessedRowIds.addAll(activeRowIds);
      activeRowIds.clear();
      return -1L;
    }

    if (cols.stream().anyMatch(col -> env.getPureStandardColumnShard(col.getName()) != null)) {
      maxRowId = cols.stream().filter(col -> env.getPureStandardColumnShard(col.getName()) != null)
          .mapToLong(column -> column.getFirstRowId()
              + env.getPureStandardColumnShard(column.getName()).getNumberOfRowsInColumnShard() - 1)
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
