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
package org.diqube.data.flatten;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.diqube.data.table.Table;
import org.diqube.data.table.TableShard;

/**
 * A flattened {@link Table}, which is based on a delegate normal {@link Table} but was flattened on a specific
 * (repeated) column.
 * 
 * @author Bastian Gloeckle
 */
public class FlattenedTable implements Table {

  private String name;
  private Collection<TableShard> shards;
  private Set<Long> originalFirstRowIdsOfShards;

  /* package */ FlattenedTable(String name, Collection<TableShard> shards, Set<Long> originalFirstRowIdsOfShards) {
    this.name = name;
    this.shards = shards;
    this.originalFirstRowIdsOfShards = originalFirstRowIdsOfShards;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<TableShard> getShards() {
    return Collections.unmodifiableCollection(shards);
  }

  /**
   * The values of {@link TableShard#getLowestRowId()} of those tableShards that this flattening was based upon.
   * 
   * <p>
   * Use these values to identify if a flattening of a table is still "valid": As each (not-flattened) Table might
   * grow/shrink (by un-/loading shards of it), the flattening might get outdated (because it is based on an od version
   * of the base table). Use these values to check if the source table still has those table shards loaded that were
   * loaded when the flattened version was created (this works, because a TableShard with a fixed firstRowId never
   * changes).
   */
  public Set<Long> getOriginalFirstRowIdsOfShards() {
    return originalFirstRowIdsOfShards;
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    long shardsSize = 0L;
    for (TableShard shard : shards)
      shardsSize += shard.calculateApproximateSizeInBytes();

    return 16 + //
        shardsSize + //
        originalFirstRowIdsOfShards.size() * 16;
  }
}
