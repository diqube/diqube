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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.diqube.context.Profiles;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.util.BigByteBuffer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class FlattenedTableUtilTest {
  private static final String TABLE = "testTable";
  private AnnotationConfigApplicationContext dataContext;
  private FlattenedTableUtil flattenedTableUtil;
  private JsonLoader loader;
  private TableFactory tableFactory;
  private Flattener flattener;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.UNIT_TEST);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    flattenedTableUtil = dataContext.getBean(FlattenedTableUtil.class);
    loader = dataContext.getBean(JsonLoader.class);
    tableFactory = dataContext.getBean(TableFactory.class);
    flattener = dataContext.getBean(Flattener.class);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void adjustRowIdsDoesNotAffectOriginal() throws LoadException {
    // GIVEN
    String json = "[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 1, \"d\":[99, 100] }, "//
    /* */ + "{ \"b\": 2, \"d\":[] }"//
        + "]" + //
        ",\"c\" : [ 9, 10 ] }," //
    //
        + "{ " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 3, \"d\":[300,301,302] }, "//
    /* */ + "{ \"b\": 4, \"d\":[303,304,305] }, "//
    /* */ + "{ \"b\": 5, \"d\":[306,307,308] } "//
        + "]" + ",\"c\" : [ 0 ]}" + " ]";

    Table origTable = loadFromJson(100L, json);
    FlattenedTable flattenedTable = flattener.flattenTable(origTable, null, "a[*]", UUID.randomUUID());

    // assume flattenedTable was rowId-adjusted.
    for (StandardColumnShard colShard : flattenedTable.getShards().iterator().next().getColumns().values()) {
      ((AdjustableStandardColumnShard) colShard).adjustToFirstRowId(200L);
    }

    // WHEN
    FlattenedTable facadedTable =
        flattenedTableUtil.facadeWithDefaultRowIds(flattenedTable, TABLE, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1L, "Expected correct flattened shards size.");
    assertValidFirstRowId(flattenedTable.getShards().iterator().next(), 200L);

    Assert.assertEquals(facadedTable.getShards().size(), 1L, "Expected correct facaded shards size.");
    assertValidFirstRowId(facadedTable.getShards().iterator().next(), 100L);

    // WHEN adjust firstRowId in facade
    for (StandardColumnShard colShard : facadedTable.getShards().iterator().next().getColumns().values()) {
      ((AdjustableStandardColumnShard) colShard).adjustToFirstRowId(500L);
    }

    // THEN
    assertValidFirstRowId(flattenedTable.getShards().iterator().next(), 200L); // flattenedTable did not change.
    assertValidFirstRowId(facadedTable.getShards().iterator().next(), 500L);
  }

  private void assertValidFirstRowId(TableShard tableShard, long firstRowId) {
    Assert.assertEquals(tableShard.getLowestRowId(), firstRowId,
        "Table shard should have same firstRowId as original.");

    Set<Long> allColumnShardsFirstRowId =
        tableShard.getColumns().values().stream().map(colShard -> colShard.getFirstRowId()).collect(Collectors.toSet());
    Assert.assertEquals(allColumnShardsFirstRowId, new HashSet<>(Arrays.asList(firstRowId)),
        "Each col shard should have the same firstRowId");

    for (StandardColumnShard colShard : tableShard.getColumns().values()) {
      long nextFirstRowId = firstRowId;
      for (ColumnPage page : colShard.getPages().values()) {
        Assert.assertEquals(page.getFirstRowId(), nextFirstRowId, "Expected correct firstRowId for pages of column "
            + colShard.getName() + ", inspected page " + page.getName());
        nextFirstRowId += page.size();
      }
    }
  }

  private Table loadFromJson(long firstRowId, String json) throws LoadException {
    BigByteBuffer jsonBuffer = new BigByteBuffer(json.getBytes(Charset.forName("UTF-8")));
    TableShard shard =
        loader.load(firstRowId, jsonBuffer, TABLE, new LoaderColumnInfo(ColumnType.LONG)).iterator().next();
    return tableFactory.createDefaultTable(TABLE, Arrays.asList(shard));
  }
}
