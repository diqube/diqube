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

import org.diqube.data.column.ColumnType;
import org.diqube.data.table.TableShard;
import org.diqube.metadata.create.TableShardMetadataBuilder;
import org.diqube.metadata.create.TableShardMetadataBuilder.IllegalTableShardLayoutException;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link TableShardMetadataBuilder}.
 *
 * @author Bastian Gloeckle
 */
public class TableShardMetadataBuilderTest {

  private TableShardMetadataBuilder builder;

  @BeforeMethod
  public void before() {
    builder = new TableShardMetadataBuilder(new RepeatedColumnNameGenerator());
  }

  @Test
  public void simpleTest() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table = TableMetadataTestUtil.mockShard(new Pair<>("a", ColumnType.STRING),
        new Pair<>("b", ColumnType.LONG), new Pair<>("c", ColumnType.DOUBLE));

    // WHEN
    TableMetadata metadata = builder.from(table).build();

    // THEN
    Assert.assertEquals(metadata.getTableName(), TableMetadataTestUtil.TABLE,
        "Expected table name being filled correctly");
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.STRING, false);
    TableMetadataTestUtil.assertField(metadata, "b", FieldType.LONG, false);
    TableMetadataTestUtil.assertField(metadata, "c", FieldType.DOUBLE, false);
    Assert.assertEquals(metadata.getFields().size(), 3, "Expected field info for all fields");
  }

  @Test
  public void repeatedTest() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table = TableMetadataTestUtil.mockShard(new Pair<>("a[0]", ColumnType.STRING),
        new Pair<>("a[1]", ColumnType.STRING), new Pair<>("a[length]", ColumnType.LONG));

    // WHEN
    TableMetadata metadata = builder.from(table).build();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.STRING, true);
    Assert.assertEquals(metadata.getFields().size(), 1, "Expected field info for all fields");
  }

  @Test
  public void containerTest() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table = TableMetadataTestUtil.mockShard(new Pair<>("a.b", ColumnType.STRING),
        new Pair<>("a.c", ColumnType.LONG), new Pair<>("a.d", ColumnType.DOUBLE));

    // WHEN
    TableMetadata metadata = builder.from(table).build();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.CONTAINER, false);
    TableMetadataTestUtil.assertField(metadata, "a.b", FieldType.STRING, false);
    TableMetadataTestUtil.assertField(metadata, "a.c", FieldType.LONG, false);
    TableMetadataTestUtil.assertField(metadata, "a.d", FieldType.DOUBLE, false);
    Assert.assertEquals(metadata.getFields().size(), 4, "Expected field info for all fields");
  }

  @Test
  public void containerRepeated1Test() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table = TableMetadataTestUtil.mockShard(new Pair<>("a.b[0]", ColumnType.LONG),
        new Pair<>("a.b[length]", ColumnType.LONG));

    // WHEN
    TableMetadata metadata = builder.from(table).build();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.CONTAINER, false);
    TableMetadataTestUtil.assertField(metadata, "a.b", FieldType.LONG, true);
    Assert.assertEquals(metadata.getFields().size(), 2, "Expected field info for all fields");
  }

  @Test
  public void containerRepeated2Test() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table = TableMetadataTestUtil.mockShard(new Pair<>("a[0].b", ColumnType.DOUBLE),
        new Pair<>("a[1].b", ColumnType.DOUBLE), new Pair<>("a[length]", ColumnType.LONG));

    // WHEN
    TableMetadata metadata = builder.from(table).build();

    // THEN
    TableMetadataTestUtil.assertField(metadata, "a", FieldType.CONTAINER, true);
    TableMetadataTestUtil.assertField(metadata, "a.b", FieldType.DOUBLE, false);
    Assert.assertEquals(metadata.getFields().size(), 2, "Expected field info for all fields");
  }

  @Test(expectedExceptions = IllegalTableShardLayoutException.class)
  public void illegalTest() throws IllegalTableShardLayoutException {
    // GIVEN
    TableShard table =
        TableMetadataTestUtil.mockShard(new Pair<>("a", ColumnType.STRING), new Pair<>("a.b", ColumnType.LONG));

    // WHEN
    builder.from(table).build();

    // THEN
    // exception expected
  }

}
