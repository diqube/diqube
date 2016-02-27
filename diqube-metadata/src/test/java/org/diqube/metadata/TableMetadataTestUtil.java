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
package org.diqube.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.diqube.data.column.ColumnType;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.table.TableShard;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.mockito.Mockito;
import org.testng.Assert;

/**
 *
 * @author Bastian Gloeckle
 */
public class TableMetadataTestUtil {
  public static final String TABLE = "tab";
  public static final long LOWEST_ROW_ID = 100L;

  @SafeVarargs
  public static final TableShard mockShard(Pair<String, ColumnType>... targetCols) {
    TableShard res = Mockito.mock(TableShard.class);
    Map<String, StandardColumnShard> cols = new HashMap<>();

    for (Pair<String, ColumnType> p : targetCols) {
      StandardColumnShard col = Mockito.mock(StandardColumnShard.class);

      Mockito.when(col.getName()).thenReturn(p.getLeft());
      Mockito.when(col.getColumnType()).thenReturn(p.getRight());

      cols.put(p.getLeft(), col);
    }

    Mockito.when(res.getColumns()).thenReturn(cols);
    Mockito.when(res.getTableName()).thenReturn(TABLE);
    Mockito.when(res.getLowestRowId()).thenReturn(LOWEST_ROW_ID);
    return res;
  }

  @SafeVarargs
  public final static TableMetadata createMetadata(Triple<String, FieldType, Boolean>... targetFields) {
    List<FieldMetadata> fields = new ArrayList<>();

    for (Triple<String, FieldType, Boolean> t : targetFields)
      fields.add(new FieldMetadata(t.getLeft(), t.getMiddle(), t.getRight()));

    return new TableMetadata(TABLE, fields);
  }

  public static void assertField(TableMetadata tableMetadata, String fieldName, FieldType fieldType,
      boolean isRepeated) {
    Optional<FieldMetadata> field =
        tableMetadata.getFields().stream().filter(f -> f.getFieldName().equals(fieldName)).findAny();
    Assert.assertTrue(field.isPresent(), "Expected to have metadata for field '" + fieldName + "'");
    Assert.assertEquals(field.get().getFieldType(), fieldType,
        "Expected correct fieldType for field '" + fieldName + "'");
    Assert.assertEquals(field.get().isRepeated(), isRepeated,
        "Expected correct isRepeated for field '" + fieldName + "'");
    Assert.assertEquals(field.get().getFieldName(), fieldName,
        "Expected correct fieldName in FieldMetadata object for field '" + fieldName + "'");
  }
}
