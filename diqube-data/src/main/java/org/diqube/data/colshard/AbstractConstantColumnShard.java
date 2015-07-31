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
package org.diqube.data.colshard;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.data.Dictionary;
import org.diqube.util.Pair;

/**
 * Abstract base implementation of a {@link ConstantColumnShard}.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractConstantColumnShard implements ConstantColumnShard {
  protected Dictionary<?> columnShardDictionary;

  protected String name;

  protected ColumnType columnType;

  private long firstRowId;

  private Object value;

  /**
   * @param columnType
   *          Data type of the column
   * @param name
   *          Name of the column
   * @param value
   *          The constant value that should be used for all rows.
   * @param firstRowId
   *          The first row ID of this {@link ColumnShard}.
   */
  protected AbstractConstantColumnShard(ColumnType columnType, String name, Object value, long firstRowId) {
    this.columnType = columnType;
    this.name = name;
    this.value = value;
    this.firstRowId = firstRowId;
    this.columnShardDictionary = createColumnShardDictionary(value);
  }

  /**
   * Create a Dictionary which will be the column shard dictionary. This needs to map the given value to ColumnValue ID
   * 0L.
   */
  abstract protected Dictionary<?> createColumnShardDictionary(Object value);

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Dictionary<?> getColumnShardDictionary() {
    return columnShardDictionary;
  }

  @Override
  public ColumnType getColumnType() {
    return columnType;
  }

  @Override
  public long getFirstRowId() {
    return firstRowId;
  }

  @Override
  public Long[] resolveColumnValueIdsForRowsFlat(Long[] rowIds) {
    Long[] res = new Long[rowIds.length];
    Arrays.fill(res, 0L);

    return res;
  }

  @Override
  public long resolveColumnValueIdForRow(Long rowId) {
    return 0;
  }

  @Override
  public Map<Long, Long> resolveColumnValueIdsForRows(Collection<Long> rowIds) {
    Map<Long, Long> res = new HashMap<>();
    rowIds.forEach(rowId -> res.put(rowId, 0L));
    return res;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public long getSingleColumnDictId() {
    return 0L; // constant, as created by #createColumnShardDictionary
  }

  @Override
  public Set<Pair<Long, Integer>> getGoodResolutionPairs() {
    return new HashSet<>(Arrays.asList(new Pair<>(firstRowId, 1)));
  }

}
