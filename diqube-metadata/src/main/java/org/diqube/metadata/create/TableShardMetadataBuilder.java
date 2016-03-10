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
package org.diqube.metadata.create;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.table.TableShard;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;

/**
 * Creates {@link TableShardMetadata} based on a single local {@link TableShard}.
 *
 * @author Bastian Gloeckle
 */
public class TableShardMetadataBuilder {
  private TableShard tableShard;
  private RepeatedColumnNameGenerator repeatedColumnNameGenerator;

  public TableShardMetadataBuilder(RepeatedColumnNameGenerator repeatedColumnNameGenerator) {
    this.repeatedColumnNameGenerator = repeatedColumnNameGenerator;
  }

  public TableShardMetadataBuilder from(TableShard tableShard) {
    this.tableShard = tableShard;
    return this;
  }

  public TableMetadata build() throws IllegalTableShardLayoutException {
    Map<String, Pair<FieldType, Boolean>> fields = new HashMap<>();

    for (ColumnShard colShard : tableShard.getColumns().values()) {
      if (FieldUtil.columnTypeMightDifferFromFieldType(colShard.getName()))
        // ignore type of length columns - we're only interested in the type of the columns that contain actual data.
        continue;

      String fieldName = FieldUtil.toFieldName(colShard.getName());
      FieldType fieldType = FieldUtil.toFieldType(colShard.getColumnType());
      boolean repeated = colShard.getName().endsWith(repeatedColumnNameGenerator.repeatedColumnNameEndsWith());

      Pair<FieldType, Boolean> newFieldInfo = new Pair<>(fieldType, repeated);
      safePutFields(fieldName, newFieldInfo, fields);

      for (Pair<String, Boolean> parentField : allParentFields(colShard.getName())) {
        Pair<FieldType, Boolean> parentFieldInfo = new Pair<>(FieldType.CONTAINER, parentField.getRight());
        safePutFields(parentField.getLeft(), parentFieldInfo, fields);
      }
    }

    // create final objects
    List<FieldMetadata> resFields = new ArrayList<>();
    for (Entry<String, Pair<FieldType, Boolean>> e : fields.entrySet())
      resFields.add(new FieldMetadata(e.getKey(), e.getValue().getLeft(), e.getValue().getRight()));

    return new TableMetadata(tableShard.getTableName(), resFields);
  }

  /**
   * @return Collection of parent field name and information if parent field seems to be repeated. Created from the
   *         column name.
   */
  private Collection<Pair<String, Boolean>> allParentFields(String columnName) {
    List<Pair<String, Boolean>> res = new ArrayList<>();

    String[] allParts = columnName.split("\\.");
    String last = null;
    for (int i = 0; i < allParts.length - 1 /* leave last one out, since we only want parents */; i++) {
      String cur;
      if (last == null)
        cur = allParts[i];
      else
        cur = last + "." + allParts[i];
      boolean repeated = cur.endsWith(repeatedColumnNameGenerator.repeatedColumnNameEndsWith());
      cur = FieldUtil.toFieldName(cur);
      res.add(new Pair<>(cur, repeated));
      last = cur;
    }

    return res;
  }

  /**
   * Put new field info into map, but first check if the map contains different information first and throw information
   * if that is the case.
   */
  private void safePutFields(String fieldName, Pair<FieldType, Boolean> newFieldInfo,
      Map<String, Pair<FieldType, Boolean>> target) throws IllegalTableShardLayoutException {
    if (target.containsKey(fieldName) && !newFieldInfo.equals(target.get(fieldName)))
      throw new IllegalTableShardLayoutException("Field " + fieldName + " has incompatible types in shard "
          + tableShard.getLowestRowId() + ": " + target.get(fieldName) + " <-> " + newFieldInfo);

    target.putIfAbsent(fieldName, newFieldInfo);
  }

  /**
   * The TableShardMetadata cannot be created because the TableShard contains invalid data.
   *
   * @author Bastian Gloeckle
   */
  public static class IllegalTableShardLayoutException extends Exception {
    private static final long serialVersionUID = 1L;

    private IllegalTableShardLayoutException(String msg) {
      super(msg);
    }

    private IllegalTableShardLayoutException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
