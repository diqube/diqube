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
package org.diqube.data.lng;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.lng.array.BitEfficientLongArray;
import org.diqube.data.lng.array.ExplorableCompressedLongArray;
import org.diqube.data.lng.array.ReferenceBasedLongArray;
import org.diqube.data.lng.array.RunLengthLongArray;
import org.diqube.data.lng.dict.ArrayCompressedLongDictionary;
import org.diqube.data.lng.dict.ConstantLongDictionary;
import org.diqube.data.lng.dict.EmptyLongDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.data.serialize.DataDeserializer;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test which serializes and deserializes long columns.
 *
 * @author Bastian Gloeckle
 */
public class LongColumnSerializationTest {
  private static final String TABLE = "Test";
  private static final String COL = "col";
  private static final ObjectDoneConsumer NOOP = (a) -> {
  };

  private AnnotationConfigApplicationContext dataContext;

  private DataSerializationManager serializationManager;

  private LongColumnShardFactory longColumnShardFactory;
  private ColumnPageFactory columnPageFactory;
  private TableFactory tableFactory;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    serializationManager = dataContext.getBean(DataSerializationManager.class);
    longColumnShardFactory = dataContext.getBean(LongColumnShardFactory.class);
    columnPageFactory = dataContext.getBean(ColumnPageFactory.class);
    tableFactory = dataContext.getBean(TableFactory.class);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void testBitEfficient() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(1, createBitEfficientDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testBitEfficientMultiplePages() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(2, createBitEfficientDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testRle() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(1, createRleDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testRef() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(1, createRefDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testRleBitEfficient() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(1, createRleBitEfficientDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testRefBitEfficient() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(1, createRefBitEfficientDict(new long[] { 0, 1, 2, 3, 4, 5 }));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testConstantDict() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(2, new ConstantLongDictionary(25L, 0L));

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  @Test
  public void testEmptyDict() throws SerializationException, DeserializationException {
    // GIVEN
    Pair<TableShard, Integer> p = createTableShard(2, new EmptyLongDictionary());

    // WHEN serialize & deserialze
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataSerializer serializer = serializationManager.createSerializer();
    Map<Long, Long> valuesBefore = getAllValues(p.getLeft(), p.getRight());
    serializer.serialize(p.getLeft(), outStream, NOOP);
    DataDeserializer deserializer = serializationManager.createDeserializer();
    TableShard deserialized = (TableShard) ((DataSerialization<?>) deserializer.deserialize(TableShard.class,
        new ByteArrayInputStream(outStream.toByteArray())));

    // THEN
    Map<Long, Long> valuesAfter = getAllValues(deserialized, p.getRight());

    Assert.assertEquals(valuesAfter, valuesBefore, "Expected column to contain the same values after deserializing");
  }

  private Map<Long, Long> getAllValues(TableShard tableShard, long numberOfRowIds) {
    StandardColumnShard shard = tableShard.getColumns().get(COL);
    Map<Long, Long> res = new HashMap<>();
    for (long rowId = 0; rowId < numberOfRowIds; rowId++) {
      Entry<Long, ColumnPage> pageEntry = shard.getPages().floorEntry(rowId);
      long colPageId = pageEntry.getValue().getValues().get((int) (rowId - pageEntry.getKey()));
      long colShardId = pageEntry.getValue().getColumnPageDict().decompressValue(colPageId);
      Long value = (Long) shard.getColumnShardDictionary().decompressValue(colShardId);
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
  private Pair<TableShard, Integer> createTableShard(int numberOfColPages, LongDictionary<?> columnDict) {
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

    LongStandardColumnShard col = longColumnShardFactory.createStandardLongColumnShard(COL, colPages, columnDict);

    TableShard ts = tableFactory.createTableShard(TABLE, new ArrayList<>(Arrays.asList(col)));

    return new Pair<>(ts, numberOfRowsPerShard * colPages.size());
  }

  private ArrayCompressedLongDictionary createBitEfficientDict(long[] longDictValues) {
    return new ArrayCompressedLongDictionary(new BitEfficientLongArray(longDictValues, true));
  }

  private ArrayCompressedLongDictionary createRleDict(long[] longDictValues) {
    return new ArrayCompressedLongDictionary(new RunLengthLongArray(longDictValues, true));
  }

  private ArrayCompressedLongDictionary createRefDict(long[] longDictValues) {
    return new ArrayCompressedLongDictionary(new ReferenceBasedLongArray(longDictValues, true));
  }

  private ArrayCompressedLongDictionary createRefBitEfficientDict(long[] longDictValues) {
    ReferenceBasedLongArray ref = new ReferenceBasedLongArray();
    ref.compress(longDictValues, true, new Supplier<ExplorableCompressedLongArray<?>>() {
      @Override
      public ExplorableCompressedLongArray<?> get() {
        return new BitEfficientLongArray();
      }
    });
    return new ArrayCompressedLongDictionary(ref);
  }

  private ArrayCompressedLongDictionary createRleBitEfficientDict(long[] longDictValues) {
    RunLengthLongArray rle = new RunLengthLongArray();
    rle.compress(longDictValues, true, new Supplier<ExplorableCompressedLongArray<?>>() {
      @Override
      public ExplorableCompressedLongArray<?> get() {
        return new BitEfficientLongArray();
      }
    });
    return new ArrayCompressedLongDictionary(rle);
  }
}
