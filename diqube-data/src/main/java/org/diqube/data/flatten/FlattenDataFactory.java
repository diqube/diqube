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
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.dbl.DoubleColumnShardFactory;
import org.diqube.data.types.dbl.FlattenedDoubleStandardColumnShard;
import org.diqube.data.types.dbl.dict.DoubleDictionary;
import org.diqube.data.types.lng.FlattenedLongStandardColumnShard;
import org.diqube.data.types.lng.LongColumnShardFactory;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;
import org.diqube.data.types.str.FlattenedStringStandardColumnShard;
import org.diqube.data.types.str.StringColumnShardFactory;
import org.diqube.data.types.str.dict.StringDictionary;

/**
 * Factory for data classes that are used in conjunction with flattenning tables.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenDataFactory {

  @Inject
  private DoubleColumnShardFactory doubleFactory;

  @Inject
  private LongColumnShardFactory longFactory;

  @Inject
  private StringColumnShardFactory stringFactory;

  public FlattenedTable createFlattenedTable(String name, Collection<TableShard> shards,
      Set<Long> originalFirstRowIdsOfShards) {
    return new FlattenedTable(name, shards, originalFirstRowIdsOfShards);
  }

  public FlattenedIndexRemovingColumnPage createFlattenedIndexRemovingColumnPage(String name,
      LongDictionary<?> colPageDict, ColumnPage delegatePage, CompressedLongArray<?> sortedRemoveIndices,
      long firstRowId) {
    return new FlattenedIndexRemovingColumnPage(name, colPageDict, delegatePage, sortedRemoveIndices, firstRowId);
  }

  public FlattenedDoubleStandardColumnShard createFlattenedDoubleStandardColumnShard(String name,
      DoubleDictionary<?> columnShardDict, long firstRowId, List<ColumnPage> pages) {
    return doubleFactory.createFlattenedDoubleStandardColumnShard(name, columnShardDict, firstRowId, pages);
  }

  public FlattenedLongStandardColumnShard createFlattenedLongStandardColumnShard(String name,
      LongDictionary<?> columnShardDict, long firstRowId, List<ColumnPage> pages) {
    return longFactory.createFlattenedLongStandardColumnShard(name, columnShardDict, firstRowId, pages);
  }

  public FlattenedStringStandardColumnShard createFlattenedStringStandardColumnShard(String name,
      StringDictionary<?> columnShardDict, long firstRowId, List<ColumnPage> pages) {
    return stringFactory.createFlattenedStringStandardColumnShard(name, columnShardDict, firstRowId, pages);
  }

  public FlattenedTableShard createFlattenedTableShard(String tableName, Collection<StandardColumnShard> colShards) {
    return new FlattenedTableShard(tableName, colShards);
  }

  public FlattenedConstantColumnPage createFlattenedConstantColumnPage(String name,
      AdjustableConstantLongDictionary<?> colPageDict, long firstRowId, int rows) {
    return new FlattenedConstantColumnPage(name, colPageDict, firstRowId, rows);
  }

  public FlattenedDelegateLongDictionary createFlattenedDelegateLongDictionary(LongDictionary<?> delegate) {
    return new FlattenedDelegateLongDictionary(delegate);
  }

  public AdjustableConstantLongDictionary<?> createAdjustableConstantLongDictionary(long value) {
    return new ConstantLongDictionary(value, 0L);
  }
}
