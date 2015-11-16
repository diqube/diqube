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
package org.diqube.flatten;

import java.util.Set;

import org.diqube.data.flatten.FlattenedTable;

/**
 * Caches {@link FlattenedTable}s on disk.
 *
 * @author Bastian Gloeckle
 */
public interface FlattenedTableDiskCache {
  /**
   * Loads a FlattenedTable from disk if it exists.
   * 
   * <p>
   * Note that <b>nothing</b> is guaranteed for the returned {@link FlattenedTable} for all, the firstRowIds of the
   * tableShards, for the flattenId that was used when the {@link FlattenedTable} was flattened nor for the name of the
   * returned table. These values typically have to be adjusted, e.g. using
   * {@link FlattenedTableUtil#facadeWithDefaultRowIds(FlattenedTable, String, String, java.util.UUID)}. The returned
   * object instance <b>must not</b> be adjusted directly!
   * 
   * @param sourceTableName
   *          Name of the source table that was flattened.
   * @param flattenBy
   *          The field by which the source table was flattened.
   * @param originalFirstRowIdsOfShards
   *          Value of {@link FlattenedTable#getOriginalFirstRowIdsOfShards()} that is expected for a returned
   *          {@link FlattenedTable}.
   * @return Either a {@link FlattenedTable} or <code>null</code> if there is no cached {@link FlattenedTable} available
   *         with the given information.
   */
  public FlattenedTable load(String sourceTableName, String flattenBy, Set<Long> originalFirstRowIdsOfShards);

  /**
   * Offer a new {@link FlattenedTable} to be cached on disk.
   * 
   * @param flattenedTable
   *          The {@link FlattenedTable} that is offered to be cached.
   * @param sourceTableName
   *          The name of the table that was the source of flattening.
   * @param flattenBy
   *          The field the source table was flattened by.
   */
  public void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy);
}
