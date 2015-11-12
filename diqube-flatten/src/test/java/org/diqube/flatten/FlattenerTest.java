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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.LongStream;

import org.diqube.context.Profiles;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.lng.LongColumnShard;
import org.diqube.executionenv.querystats.QueryableLongColumnShardFacade;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.util.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link Flattener}.
 *
 * @author Bastian Gloeckle
 */
public class FlattenerTest {
  private static final Logger logger = LoggerFactory.getLogger(FlattenerTest.class);

  private static final String TABLE = "table";
  private static final Comparator<SortedMap<String, Long>> MAP_COMPARATOR = new Comparator<SortedMap<String, Long>>() {
    @Override
    public int compare(SortedMap<String, Long> o1, SortedMap<String, Long> o2) {
      Iterator<Entry<String, Long>> i1 = o1.entrySet().iterator();
      Iterator<Entry<String, Long>> i2 = o2.entrySet().iterator();

      while (i1.hasNext()) {
        if (!i2.hasNext())
          return 1; // i2 is shorter

        Entry<String, Long> e1 = i1.next();
        Entry<String, Long> e2 = i2.next();

        if (e1.getKey().compareTo(e2.getKey()) != 0)
          return e1.getKey().compareTo(e2.getKey());

        if (e1.getValue().compareTo(e2.getValue()) != 0)
          return e1.getValue().compareTo(e2.getValue());
      }
      if (i2.hasNext())
        return -1; // i1 is shorter
      return 0;
    }
  };

  private AnnotationConfigApplicationContext dataContext;
  private JsonLoader loader;
  private TableFactory tableFactory;

  private Flattener flattener;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    loader = dataContext.getBean(JsonLoader.class);
    tableFactory = dataContext.getBean(TableFactory.class);
    flattener = dataContext.getBean(Flattener.class);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void simpleTest() throws LoadException {
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

    // GIVEN
    Table t = loadFromJson(json);

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 0L, "Expected correct lowest row ID");
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 5, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(),
        new HashSet<>(Arrays.asList("a.b", "a.d[0]", "a.d[1]", "a.d[2]", "a.d[length]", "c[0]", "c[1]", "c[length]")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row = new TreeMap<>();
    row.put("a.b", 1L);
    row.put("a.d[0]", 99L);
    row.put("a.d[1]", 100L);
    row.put("a.d[2]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[length]", 2L);
    row.put("c[0]", 9L);
    row.put("c[1]", 10L);
    row.put("c[length]", 2L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 2L);
    row.put("a.d[0]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[2]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[length]", 0L);
    row.put("c[0]", 9L);
    row.put("c[1]", 10L);
    row.put("c[length]", 2L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 3L);
    row.put("a.d[0]", 300L);
    row.put("a.d[1]", 301L);
    row.put("a.d[2]", 302L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 4L);
    row.put("a.d[0]", 303L);
    row.put("a.d[1]", 304L);
    row.put("a.d[2]", 305L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 5L);
    row.put("a.d[0]", 306L);
    row.put("a.d[1]", 307L);
    row.put("a.d[2]", 308L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  @Test
  public void repeatedMiddleMissingTest() throws LoadException {
    // GIVEN
    Table t = loadFromJson("[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 1 }," //
    /*   */ + "{ \"d\": 1 }," //
    /*   */ + "{ \"c\": 3 }" //
    /* */ + "] }, "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 2 }," //
    /*   */ + "{ \"d\": 2 }," //
    /*   */ + "{ \"c\": 4 }" //
    /* */ + "] } "//
        + "]" + //
        "},"
        //
        + "{ " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 5 }," //
    /*   */ + "{ \"d\": 5 }," //
    /*   */ + "{ \"c\": 6 }" //
    /* */ + "] }, "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 7 }," //
    /*   */ + "{ \"d\": 7 }," //
    /*   */ + "{ \"c\": 8 }" //
    /* */ + "] } "//
        + "]" + //
        "}" + " ]");

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*].b[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 0L, "Expected correct lowest row ID");
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 12, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(), new HashSet<>(Arrays.asList("a.b.c", "a.b.d")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row = new TreeMap<>();
    row.put("a.b.c", 1L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.b.d", 1L);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 3L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 2L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.b.d", 2L);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 4L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b.c", 5L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.b.d", 5L);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 6L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 7L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.b.d", 7L);
    expectedRows.add(row);
    row = new TreeMap<>();
    row.put("a.b.c", 8L);
    row.put("a.b.d", LoaderColumnInfo.DEFAULT_LONG);
    expectedRows.add(row);

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  @Test
  public void simpleTestHighFirstRowId() throws LoadException {
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

    // GIVEN
    Table t = loadFromJson(100, json);

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 100L, "Expected correct lowest row ID"); /* same as source table */
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 5, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(),
        new HashSet<>(Arrays.asList("a.b", "a.d[0]", "a.d[1]", "a.d[2]", "a.d[length]", "c[0]", "c[1]", "c[length]")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row = new TreeMap<>();
    row.put("a.b", 1L);
    row.put("a.d[0]", 99L);
    row.put("a.d[1]", 100L);
    row.put("a.d[2]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[length]", 2L);
    row.put("c[0]", 9L);
    row.put("c[1]", 10L);
    row.put("c[length]", 2L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 2L);
    row.put("a.d[0]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[2]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("a.d[length]", 0L);
    row.put("c[0]", 9L);
    row.put("c[1]", 10L);
    row.put("c[length]", 2L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 3L);
    row.put("a.d[0]", 300L);
    row.put("a.d[1]", 301L);
    row.put("a.d[2]", 302L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 4L);
    row.put("a.d[0]", 303L);
    row.put("a.d[1]", 304L);
    row.put("a.d[2]", 305L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    row = new TreeMap<>();
    row.put("a.b", 5L);
    row.put("a.d[0]", 306L);
    row.put("a.d[1]", 307L);
    row.put("a.d[2]", 308L);
    row.put("a.d[length]", 3L);
    row.put("c[0]", 0L);
    row.put("c[1]", LoaderColumnInfo.DEFAULT_LONG);
    row.put("c[length]", 1L);
    expectedRows.add(row);

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  @Test
  public void simpleTestForceHalfRemoval() throws LoadException {
    // Test which contains a specific repetition index in approx half of the rows -> should trigger a specific ColPage
    // to be built.
    // a[0] is contained in every row, a[1] only in 5 of 9.

    String json = "[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 1 }, "//
    /* */ + "{ \"b\": 2 }"//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 3 }, "//
    /* */ + "{ \"b\": 4 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 5 }, "//
    /* */ + "{ \"b\": 6 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 7 }, "//
    /* */ + "{ \"b\": 8 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 9 }, "//
    /* */ + "{ \"b\": 10 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 11 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 12 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 13 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 14 } "//
        + "]}" //
    //
        + " ]";

    // GIVEN
    Table t = loadFromJson(100, json);

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 100L, "Expected correct lowest row ID"); /* same as source table */
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 14, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(), new HashSet<>(Arrays.asList("a.b")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row;
    for (long l = 1L; l <= 14; l++) {
      row = new TreeMap<>();
      row.put("a.b", l);
      expectedRows.add(row);
    }

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  @Test
  public void simpleTestForceLittleRemoval() throws LoadException {
    // Test which contains a specific repetition index in most of the rows -> should trigger a specific ColPage
    // to be built.
    // a[0] is contained in every row, a[1] only in 1 of 9.

    String json = "[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 1 }, "//
    /* */ + "{ \"b\": 2 }"//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 3 }, "//
    /* */ + "{ \"b\": 4 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 5 }, "//
    /* */ + "{ \"b\": 6 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 7 }, "//
    /* */ + "{ \"b\": 8 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 9 }, "//
    /* */ + "{ \"b\": 10 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 11 }, "//
    /* */ + "{ \"b\": 12 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 13 }, "//
    /* */ + "{ \"b\": 14 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 15 }, "//
    /* */ + "{ \"b\": 16 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 17 } "//
        + "]}" //
    //
        + " ]";

    // GIVEN
    Table t = loadFromJson(100, json);

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 100L, "Expected correct lowest row ID"); /* same as source table */
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 17, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(), new HashSet<>(Arrays.asList("a.b")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row;
    for (long l = 1L; l <= 17; l++) {
      row = new TreeMap<>();
      row.put("a.b", l);
      expectedRows.add(row);
    }

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  @Test
  public void simpleTestForceMostRemoval() throws LoadException {
    // Test which contains a specific repetition index in only a small number of the rows -> should trigger a specific
    // ColPage to be built.
    // a[0] is contained in every row, a[1] only in 1 of 9.

    String json = "[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 1 }, "//
    /* */ + "{ \"b\": 2 }"//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 3 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 4 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 5 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 6 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 7 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 8 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 9 } "//
        + "]}," //
        + "{ \"a\": [ "//
    /* */ + "{ \"b\": 10 } "//
        + "]}" //
    //
        + " ]";

    // GIVEN
    Table t = loadFromJson(100, json);

    // WHEN
    FlattenedTable flattenedTable = flattener.flattenTable(t, null, "a[*]", UUID.randomUUID());

    // THEN
    Assert.assertEquals(flattenedTable.getShards().size(), 1, "Expected correct table shard count");
    TableShard tableShard = flattenedTable.getShards().iterator().next();
    Assert.assertEquals(tableShard.getLowestRowId(), 100L, "Expected correct lowest row ID"); /* same as source table */
    Assert.assertEquals(tableShard.getNumberOfRowsInShard(), 10, "Expected correct number of rows.");

    Assert.assertEquals(tableShard.getColumns().keySet(), new HashSet<>(Arrays.asList("a.b")),
        "Expected correct columns.");

    SortedSet<SortedMap<String, Long>> expectedRows = new TreeSet<>(MAP_COMPARATOR);

    SortedMap<String, Long> row;
    for (long l = 1L; l <= 10; l++) {
      row = new TreeMap<>();
      row.put("a.b", l);
      expectedRows.add(row);
    }

    Assert.assertEquals(getAllRows(tableShard), expectedRows, "Expected to have correct rows.");
  }

  private Table loadFromJson(String json) throws LoadException {
    return loadFromJson(0, json);
  }

  private Table loadFromJson(long firstRowId, String json) throws LoadException {
    BigByteBuffer jsonBuffer = new BigByteBuffer(json.getBytes(Charset.forName("UTF-8")));
    TableShard shard =
        loader.load(firstRowId, jsonBuffer, TABLE, new LoaderColumnInfo(ColumnType.LONG)).iterator().next();
    return tableFactory.createDefaultTable(TABLE, Arrays.asList(shard));
  }

  private SortedSet<SortedMap<String, Long>> getAllRows(TableShard tableShard) {
    Map<Long, SortedMap<String, Long>> rows = new HashMap<>();
    LongStream.range(tableShard.getLowestRowId(), tableShard.getLowestRowId() + tableShard.getNumberOfRowsInShard())
        .forEach(rowId -> rows.put(rowId, new TreeMap<>()));

    for (String colName : tableShard.getColumns().keySet()) {
      ColumnShard col = tableShard.getColumns().get(colName);
      QueryableLongColumnShardFacade queryableCol = new QueryableLongColumnShardFacade((LongColumnShard) col);

      Map<Long, Long> valueIdsByRow = queryableCol.resolveColumnValueIdsForRows(rows.keySet());
      for (Entry<Long, Long> e : valueIdsByRow.entrySet()) {
        rows.get(e.getKey()).put(colName, queryableCol.getColumnShardDictionary().decompressValue(e.getValue()));
      }
    }

    SortedSet<SortedMap<String, Long>> res = new TreeSet<>(MAP_COMPARATOR);
    res.addAll(rows.values());
    return res;
  }

  // private List<Long> resolveValues(TableShard tableShard, String colName, List<Long> rowIds) {
  // QueryableLongColumnShardFacade col =
  // new QueryableLongColumnShardFacade((LongColumnShard) tableShard.getColumns().get(colName));
  // Long[] colValueIds = col.resolveColumnValueIdsForRowsFlat(rowIds);
  // return Arrays.asList(col.getColumnShardDictionary().decompressValues(colValueIds));
  // }
}
