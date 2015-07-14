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
package org.diqube.loader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
import org.diqube.data.lng.LongStandardColumnShard;
import org.diqube.util.BigByteBuffer;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link JsonLoader}.
 *
 * @author Bastian Gloeckle
 */
public class JsonLoaderTest {
  private static final String TABLE = "Test";

  private JsonLoader loader;
  private LoaderColumnInfo colInfo;

  private AnnotationConfigApplicationContext dataContext;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    loader = dataContext.getBean(JsonLoader.class);
    colInfo = new LoaderColumnInfo(ColumnType.STRING);
  }

  @Test
  public void simpleJson() throws LoadException {
    // GIVEN
    String json = "[ { \"a\": 1, \"b\": 1},{\"a\": 2, \"b\": 3}]";

    // WHEN
    TableShard tableShard = loader.load(0L, new BigByteBuffer(json.getBytes()), TABLE, colInfo);

    // THEN
    Assert.assertEquals(tableShard.getLongColumns().size(), 2, "Expected both long columns to be available");

    Set<Pair<Long, Long>> expectedValues = new HashSet<>();
    expectedValues.add(new Pair<>(1L, 1L));
    expectedValues.add(new Pair<>(2L, 3L));

    Set<Pair<Long, Long>> actualValues = new HashSet<>();
    LongStandardColumnShard colA = tableShard.getLongColumns().get("a");
    LongStandardColumnShard colB = tableShard.getLongColumns().get("b");
    for (long i = tableShard.getLowestRowId(); i < tableShard.getLowestRowId()
        + tableShard.getNumberOfRowsInShard(); i++) {
      Long valueA = resolveSingleRowValue(colA, i);
      Long valueB = resolveSingleRowValue(colB, i);
      actualValues.add(new Pair<>(valueA, valueB));
    }

    Assert.assertEquals(actualValues, expectedValues, "Expected correct values to be encoded");
  }

  @Test
  public void simpleArrayJson() throws LoadException {
    // GIVEN
    String json = "[ { \"a\": 1, \"b\": 1, \"c\": [4, 5,6]},{\"a\": 2, \"b\": 3, \"c\": [7, 8, 9 ]}]";

    // WHEN
    TableShard tableShard = loader.load(0L, new BigByteBuffer(json.getBytes()), TABLE, colInfo);

    // THEN
    Assert.assertEquals(tableShard.getLongColumns().size(), 5, "Expected all long columns to be available");

    Set<Triple<Long, Long, List<Long>>> expectedValues = new HashSet<>();
    expectedValues.add(new Triple<>(1L, 1L, Arrays.asList(new Long[] { 4L, 5L, 6L })));
    expectedValues.add(new Triple<>(2L, 3L, Arrays.asList(new Long[] { 7L, 8L, 9L })));

    Set<Triple<Long, Long, List<Long>>> actualValues = new HashSet<>();
    LongStandardColumnShard colA = tableShard.getLongColumns().get("a");
    LongStandardColumnShard colB = tableShard.getLongColumns().get("b");
    LongStandardColumnShard colC0 = tableShard.getLongColumns().get("c[0]");
    LongStandardColumnShard colC1 = tableShard.getLongColumns().get("c[1]");
    LongStandardColumnShard colC2 = tableShard.getLongColumns().get("c[2]");
    for (long i = tableShard.getLowestRowId(); i < tableShard.getLowestRowId()
        + tableShard.getNumberOfRowsInShard(); i++) {
      Long valueA = resolveSingleRowValue(colA, i);
      Long valueB = resolveSingleRowValue(colB, i);
      Long valueC0 = resolveSingleRowValue(colC0, i);
      Long valueC1 = resolveSingleRowValue(colC1, i);
      Long valueC2 = resolveSingleRowValue(colC2, i);
      actualValues.add(new Triple<>(valueA, valueB, Arrays.asList(new Long[] { valueC0, valueC1, valueC2 })));
    }

    Assert.assertEquals(actualValues, expectedValues, "Expected correct values to be encoded");
  }

  @Test
  public void arrayContainingObjectsJson() throws LoadException {
    // GIVEN
    String json = "[ { \"a\": 1, \"c\": [{ \"d\" : 4, \"e\" : 4 }, { \"d\" : 5, \"e\" : 5 }]},"
        + "{\"a\": 2, \"c\": [ { \"d\" : 7, \"e\" : 7 }, { \"d\" : 8, \"e\" : 8 }] } ]";

    // WHEN
    TableShard tableShard = loader.load(0L, new BigByteBuffer(json.getBytes()), TABLE, colInfo);

    // THEN
    Assert.assertEquals(tableShard.getLongColumns().size(), 5, "Expected all long columns to be available");

    Set<Triple<Long, List<Long>, List<Long>>> expectedValues = new HashSet<>();
    expectedValues.add(new Triple<>(1L, Arrays.asList(new Long[] { 4L, 5L }), Arrays.asList(new Long[] { 4L, 5L })));
    expectedValues.add(new Triple<>(2L, Arrays.asList(new Long[] { 7L, 8L }), Arrays.asList(new Long[] { 7L, 8L })));

    Set<Triple<Long, List<Long>, List<Long>>> actualValues = new HashSet<>();
    LongStandardColumnShard colA = tableShard.getLongColumns().get("a");
    LongStandardColumnShard colC0D = tableShard.getLongColumns().get("c[0].d");
    LongStandardColumnShard colC1D = tableShard.getLongColumns().get("c[1].d");
    LongStandardColumnShard colC0E = tableShard.getLongColumns().get("c[0].e");
    LongStandardColumnShard colC1E = tableShard.getLongColumns().get("c[1].e");
    for (long i = tableShard.getLowestRowId(); i < tableShard.getLowestRowId()
        + tableShard.getNumberOfRowsInShard(); i++) {
      Long valueA = resolveSingleRowValue(colA, i);
      Long valueC0D = resolveSingleRowValue(colC0D, i);
      Long valueC1D = resolveSingleRowValue(colC1D, i);
      Long valueC0E = resolveSingleRowValue(colC0E, i);
      Long valueC1E = resolveSingleRowValue(colC1E, i);
      actualValues.add(new Triple<>(valueA, Arrays.asList(new Long[] { valueC0D, valueC1D }),
          Arrays.asList(new Long[] { valueC0E, valueC1E })));
    }

    Assert.assertEquals(actualValues, expectedValues, "Expected correct values to be encoded");
  }

  @Test
  public void longJson() throws LoadException {
    // GIVEN
    String json = //
        "[ { \"a\": 0,\"b\": 30 }," + //
            "{ \"a\": 1,\"b\": 23 }," + //
            "{ \"a\": 2,\"b\": 55 }," + //
            "{ \"a\": 3,\"b\": 26 }," + //
            "{ \"a\": 4,\"b\": 39 }," + //
            "{ \"a\": 5,\"b\": 36 }," + //
            "{ \"a\": 6,\"b\": 55 }," + //
            "{ \"a\": 7,\"b\": 39 }," + //
            "{ \"a\": 8,\"b\": 10 }," + //
            "{ \"a\": 9,\"b\": 25 }]";

    // WHEN
    TableShard tableShard = loader.load(0L, new BigByteBuffer(json.getBytes()), TABLE, colInfo);

    // THEN
    Assert.assertEquals(tableShard.getLongColumns().size(), 2, "Expected both long columns to be available");

    Set<Pair<Long, Long>> expectedValues = new HashSet<>();
    expectedValues.add(new Pair<>(0L, 30L));
    expectedValues.add(new Pair<>(1L, 23L));
    expectedValues.add(new Pair<>(2L, 55L));
    expectedValues.add(new Pair<>(3L, 26L));
    expectedValues.add(new Pair<>(4L, 39L));
    expectedValues.add(new Pair<>(5L, 36L));
    expectedValues.add(new Pair<>(6L, 55L));
    expectedValues.add(new Pair<>(7L, 39L));
    expectedValues.add(new Pair<>(8L, 10L));
    expectedValues.add(new Pair<>(9L, 25L));

    Set<Pair<Long, Long>> actualValues = new HashSet<>();
    LongStandardColumnShard colA = tableShard.getLongColumns().get("a");
    LongStandardColumnShard colB = tableShard.getLongColumns().get("b");
    for (long i = tableShard.getLowestRowId(); i < tableShard.getLowestRowId()
        + tableShard.getNumberOfRowsInShard(); i++) {
      Long valueA = resolveSingleRowValue(colA, i);
      Long valueB = resolveSingleRowValue(colB, i);
      actualValues.add(new Pair<>(valueA, valueB));
    }

    Assert.assertEquals(actualValues, expectedValues, "Expected correct values to be encoded");
  }

  @Test(expectedExceptions = LoadException.class)
  public void unparsableJson() throws LoadException {
    // GIVEN
    String json = //
        "[ { \"a\": 0,\"b\": " + Long.MAX_VALUE + "9 }]";

    // WHEN
    loader.load(0L, new BigByteBuffer(json.getBytes()), TABLE, colInfo);

    // THEN: exception
  }

  private Long resolveSingleRowValue(LongStandardColumnShard col, long rowId) {
    return col.getColumnShardDictionary()
        .decompressValue(col.resolveColumnValueIdsForRowsFlat(new Long[] { rowId })[0]);
  }
}
