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
package org.diqube.execution.env.querystats;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.util.Pair;

/**
 * A facade for {@link LongColumnShard} that gathers query statistics.
 *
 * @author Bastian Gloeckle
 */
public class LongColumnShardStatsFacade implements LongColumnShard, ColumnShardStatsFacade {
  private LongColumnShard delegate;

  public LongColumnShardStatsFacade(LongColumnShard delegate, boolean isTempColumn) {
    this.delegate = delegate;
  }

  @Override
  public LongDictionary getColumnShardDictionary() {
    return delegate.getColumnShardDictionary();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public ColumnType getColumnType() {
    return delegate.getColumnType();
  }

  @Override
  public long getFirstRowId() {
    return delegate.getFirstRowId();
  }

  @Override
  public Map<Long, Long> resolveColumnValueIdsForRows(Collection<Long> rowIds) {
    return delegate.resolveColumnValueIdsForRows(rowIds);
  }

  @Override
  public Long[] resolveColumnValueIdsForRowsFlat(Long[] rowIds) {
    return delegate.resolveColumnValueIdsForRowsFlat(rowIds);
  }

  @Override
  public long resolveColumnValueIdForRow(Long rowId) {
    return delegate.resolveColumnValueIdForRow(rowId);
  }

  @Override
  public Set<Pair<Long, Integer>> getGoodResolutionPairs() {
    return delegate.getGoodResolutionPairs();
  }

  @Override
  public LongColumnShard getDelegate() {
    return delegate;
  }

}
