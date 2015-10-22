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
package org.diqube.data.types.str;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DataDeserializer;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.data.types.lng.array.BitEfficientLongArray;
import org.diqube.data.types.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.types.str.StringColumnShardFactory;
import org.diqube.data.types.str.StringStandardColumnShard;
import org.diqube.data.types.str.dict.ConstantStringDictionary;
import org.diqube.data.types.str.dict.ParentNode;
import org.diqube.data.types.str.dict.StringDictionary;
import org.diqube.data.types.str.dict.TrieStringDictionary;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test which serializes and deserializes string columns.
 *
 * @author Bastian Gloeckle
 */
public class StringColumnSerializationTest {
  private static final String TABLE = "Test";
  private static final String COL = "col";
  private static final ObjectDoneConsumer NOOP = (a) -> {
  };

  private AnnotationConfigApplicationContext dataContext;

  private DataSerializationManager serializationManager;

  private StringColumnShardFactory stringColumnShardFactory;
  private ColumnPageFactory columnPageFactory;
  private TableFactory tableFactory;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    serializationManager = dataContext.getBean(DataSerializationManager.class);
    stringColumnShardFactory = dataContext.getBean(StringColumnShardFactory.class);
    columnPageFactory = dataContext.getBean(ColumnPageFactory.class);
    tableFactory = dataContext.getBean(TableFactory.class);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void testSimple() throws SerializationException, DeserializationException {
    // GIVEN
    @SuppressWarnings("unchecked")
    Pair<TableShard, Integer> p = createTableShard(1,
        createTrieDict( //
            TrieTestUtil.parent( //
                new Pair<>("abc", TrieTestUtil.terminal(0L)), //
                new Pair<>("xyz", TrieTestUtil.terminal(1L))),
            "abc", "xyz", 1L));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, String> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, String> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testTwoLevel() throws SerializationException, DeserializationException {
    // GIVEN
    @SuppressWarnings("unchecked")
    Pair<TableShard, Integer> p = createTableShard(1,
        createTrieDict( //
            TrieTestUtil.parent( //
                new Pair<>("a",
                    TrieTestUtil.parent( //
                        new Pair<>("bc", TrieTestUtil.terminal(0L)), //
                        new Pair<>("cd", TrieTestUtil.terminal(1L)))), //
            new Pair<>("xyz", TrieTestUtil.terminal(2L))), "abc", "xyz", 1L));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, String> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, String> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testConstantDict() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(2, new ConstantStringDictionary("hello", 0L));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, String> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, String> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  private Map<Long, String> getAllValues(TableShard tableShard, long numberOfRowIds) {
    StandardColumnShard shard = tableShard.getColumns().get(COL);
    Map<Long, String> res = new HashMap<>();
    for (long rowId = 0; rowId < numberOfRowIds; rowId++) {
      Entry<Long, ColumnPage> pageEntry = shard.getPages().floorEntry(rowId);
      long colPageId = pageEntry.getValue().getValues().get((int) (rowId - pageEntry.getKey()));
      long colShardId = pageEntry.getValue().getColumnPageDict().decompressValue(colPageId);
      String value = (String) shard.getColumnShardDictionary().decompressValue(colShardId);
      res.put(rowId, value);
    }
    return res;
  }

  /**
   * Create a testable {@link TableShard}.
   * 
   * @param numberOfColPages
   *          Number of {@link ColumnPage}s in the {@link ColumnShard} that is created.
   * @param columnDict
   *          the Column shard dict to use.
   * @return pair of new {@link TableShard} and the number of rows that have values in the tableShard.
   */
  private Pair<TableShard, Integer> createTableShard(int numberOfColPages, StringDictionary<?> columnDict) {
    NavigableMap<Long, ColumnPage> colPages = new TreeMap<>();

    int numberOfRowsPerShard;
    if (columnDict.getMaxId() != null) {
      numberOfRowsPerShard = (columnDict.getMaxId().intValue() + 1) / numberOfColPages;
      long nextId = 0L;
      for (int i = 0; i < numberOfColPages; i++) {
        long[] pageDictArrayPlain = new long[numberOfRowsPerShard];
        long[] pageValueArrayPlain = new long[numberOfRowsPerShard];
        for (int j = 0; j < numberOfRowsPerShard; j++) {
          pageValueArrayPlain[j] = numberOfRowsPerShard - 1 - j;
          pageDictArrayPlain[j] = nextId++;
        }

        BitEfficientLongArray pageDictArray = new BitEfficientLongArray(pageDictArrayPlain, true);
        BitEfficientLongArray pageValueArray = new BitEfficientLongArray(pageValueArrayPlain, false);
        ColumnPage page = columnPageFactory.createDefaultColumnPage(new ArrayCompressedLongDictionary(pageDictArray),
            pageValueArray, i * numberOfRowsPerShard, COL + "#" + (nextId - numberOfRowsPerShard));
        colPages.put(nextId - numberOfRowsPerShard, page);
      }
    } else
      numberOfRowsPerShard = 0;

    StringStandardColumnShard col = stringColumnShardFactory.createStandardStringColumnShard(COL, colPages, columnDict);

    TableShard ts = tableFactory.createTableShard(TABLE, new ArrayList<>(Arrays.asList(col)));

    return new Pair<>(ts, numberOfRowsPerShard * colPages.size());
  }

  private TrieStringDictionary createTrieDict(ParentNode parent, String firstValue, String lastValue, long lastId) {
    return new TrieStringDictionary(parent, firstValue, lastValue, lastId);
  }

}
