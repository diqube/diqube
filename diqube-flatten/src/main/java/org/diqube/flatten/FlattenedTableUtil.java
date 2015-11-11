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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.flatten.AbstractFlattenedStandardColumnShard;
import org.diqube.data.flatten.AdjustableConstantLongDictionary;
import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedConstantColumnPage;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.flatten.FlattenedTableShard;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.dbl.DoubleStandardColumnShard;
import org.diqube.data.types.lng.LongStandardColumnShard;
import org.diqube.data.types.str.StringStandardColumnShard;
import org.diqube.name.FlattenedTableNameGenerator;

/**
 * Util for {@link FlattenedTable}s.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenedTableUtil {

  @Inject
  private FlattenDataFactory factory;

  @Inject
  private FlattenedTableNameGenerator flattenedTableNameGenerator;

  /**
   * Will facade the given table, so in the returned table, the
   * {@link AdjustableStandardColumnShard#adjustToFirstRowId(long)} can be safely executed without changing the source
   * table.
   * 
   * <p>
   * The returned table will have those firstRowIds of the table that the flattening was originally based on.
   * 
   * @param inputTable
   *          The table to be facaded.
   * @param Name
   *          of the original (not-flattened) table.
   * @param flattenBy
   *          The field which was flattened by.
   * @param flattenId
   *          The new ID of the flattening (will be used on returned table).
   * @return New table object which re-uses as much as possible from input table, but its firstRowIds can be adjusted
   *         without changing the inputTable. The returned FlattenedTables firstRowIds (of all col shards and
   *         distributed validly across all colPages) basically is in the same state as
   *         {@link Flattener#flattenTable(org.diqube.data.table.Table, java.util.Collection, String, java.util.UUID)}
   *         would return.
   */
  public FlattenedTable facadeWithDefaultRowIds(FlattenedTable inputTable, String origTableName, String flattenBy,
      UUID flattenId) {
    String newTableName = flattenedTableNameGenerator.createFlattenedTableName(origTableName, flattenBy, flattenId);

    Collection<TableShard> newTableShards = new ArrayList<>();

    List<TableShard> inputTableShardsSorted = new ArrayList<>(inputTable.getShards());
    inputTableShardsSorted.sort((s1, s2) -> Long.compare(s1.getLowestRowId(), s2.getLowestRowId()));

    Iterator<Long> origTableShardFirstRowIdIt =
        inputTable.getOriginalFirstRowIdsOfShards().stream().sorted().iterator();

    for (TableShard inputTableShardOrig : inputTableShardsSorted) {
      FlattenedTableShard inputTableShard = (FlattenedTableShard) inputTableShardOrig;
      long origFirstRowId = origTableShardFirstRowIdIt.next();
      Collection<StandardColumnShard> newColShards = new ArrayList<>();

      for (StandardColumnShard inputColumnShard : inputTableShard.getColumns().values()) {

        List<ColumnPage> newPages = new ArrayList<>();

        long nextFirstRowId = origFirstRowId;

        for (ColumnPage inputPage : inputColumnShard.getPages().values()) {
          ColumnPage newPage;
          if (inputPage instanceof FlattenedConstantColumnPage) {
            // was a constant page, simply create a new one with an adjusted firstRowId.
            newPage = factory.createFlattenedConstantColumnPage(inputColumnShard.getName() + "#" + nextFirstRowId,
                (AdjustableConstantLongDictionary<?>) ((FlattenedConstantColumnPage) inputPage).getColumnPageDict(),
                nextFirstRowId, ((FlattenedConstantColumnPage) inputPage).getRows());
          } else {
            // Use the original dict and values, but provide a different firstRowId.
            newPage = factory.createFlattenedColumnPage(inputColumnShard.getName() + "#" + nextFirstRowId,
                inputPage.getColumnPageDict(), inputPage.getValues(), nextFirstRowId);
          }
          nextFirstRowId += newPage.size();
          newPages.add(newPage);
        }

        AbstractFlattenedStandardColumnShard newColShard = null;
        switch (inputColumnShard.getColumnType()) {
        case STRING:
          newColShard = factory.createFlattenedStringStandardColumnShard(inputColumnShard.getName(),
              ((StringStandardColumnShard) inputColumnShard).getColumnShardDictionary(), origFirstRowId, newPages);
          break;
        case LONG:
          newColShard = factory.createFlattenedLongStandardColumnShard(inputColumnShard.getName(),
              ((LongStandardColumnShard) inputColumnShard).getColumnShardDictionary(), origFirstRowId, newPages);
          break;
        case DOUBLE:
          newColShard = factory.createFlattenedDoubleStandardColumnShard(inputColumnShard.getName(),
              ((DoubleStandardColumnShard) inputColumnShard).getColumnShardDictionary(), origFirstRowId, newPages);
          break;
        }
        newColShards.add(newColShard);
      }

      newTableShards.add(factory.createFlattenedTableShard(newTableName, newColShards));
    }

    return factory.createFlattenedTable(newTableName, newTableShards, inputTable.getOriginalFirstRowIdsOfShards());
  }
}
