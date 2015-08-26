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
package org.diqube.hadoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.diqube.data.ColumnType;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.data.serialize.DataSerializer;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.hadoop.DiqubeRow.DiqubeData;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.util.NullUtil;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * A {@link RecordWriter} which writes to .diqube files.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeRecordWriter extends RecordWriter<NullWritable, DiqubeRow> {

  private DataOutputStream outStream;
  private AnnotationConfigApplicationContext ctx;
  private ColumnShardBuilderManager columnShardBuilderManager;
  private AtomicLong nextRowId = new AtomicLong(0L);
  private RepeatedColumnNameGenerator repeatedColNameGen;
  private LoaderColumnInfo colInfo;
  private Set<String> repeatedLengthColNames = new ConcurrentSkipListSet<>();

  public DiqubeRecordWriter(DataOutputStream outStream) {
    this.outStream = outStream;

    ctx = new AnnotationConfigApplicationContext();
    // do not enable newDataWatcher and/or Config.
    ctx.scan("org.diqube");
    ctx.refresh();

    colInfo = new LoaderColumnInfo(ColumnType.LONG);
    columnShardBuilderManager =
        ctx.getBean(ColumnShardBuilderFactory.class).createColumnShardBuilderManager(colInfo, 0L);
    repeatedColNameGen = ctx.getBean(RepeatedColumnNameGenerator.class);
  }

  @Override
  public void write(NullWritable key, DiqubeRow row) throws IOException, InterruptedException {
    if (row.getData() == null)
      throw new IOException("No data specified.");

    try {
      row.getData().validate();
    } catch (IllegalStateException e) {
      throw new IOException("DiqubeRow not valid: " + e.getMessage(), e);
    }

    // fill in data of that single new row into columnShardBuilderManager
    long rowId = nextRowId.getAndIncrement();
    Deque<Pair<String, DiqubeData>> dataQueue = new LinkedList<>();
    dataQueue.add(new Pair<>("", row.getData()));
    while (!dataQueue.isEmpty()) {
      Pair<String, DiqubeData> dataPair = dataQueue.poll();
      DiqubeData data = dataPair.getRight();
      String parentColName = dataPair.getLeft();

      for (Entry<String, Object> fieldEntry : data.getData().entrySet()) {
        String colName;
        if ("".equals(parentColName))
          colName = fieldEntry.getKey();
        else
          colName = parentColName + "." + fieldEntry.getKey();

        if (fieldEntry.getValue() instanceof DiqubeData)
          dataQueue.add(new Pair<>(colName, (DiqubeData) fieldEntry.getValue()));
        else {
          // fill ColumnInfo just before adding the values, with this we can identify the column types dynamically.
          if (colInfo.getRegisteredColumnType(colName) == null)
            colInfo.registerColumnType(colName, objectToColType(fieldEntry.getValue()));
          else if (!colInfo.getRegisteredColumnType(colName).equals(objectToColType(fieldEntry.getValue())))
            throw new IOException("Column '" + colName + "' has at least two different data types: "
                + colInfo.getRegisteredColumnType(colName) + " <-> " + objectToColType(fieldEntry.getValue())
                + ". This is not allowed.");

          Object[] valueArray = (Object[]) Array.newInstance(fieldEntry.getValue().getClass(), 1);
          valueArray[0] = fieldEntry.getValue();
          columnShardBuilderManager.addValues(colName, valueArray, rowId);
        }
      }

      for (Entry<String, List<Object>> repeatedEntry : data.getRepeatedData().entrySet()) {
        String colName;
        if ("".equals(parentColName))
          colName = repeatedEntry.getKey();
        else
          colName = parentColName + "." + repeatedEntry.getKey();

        for (int idx = 0; idx < repeatedEntry.getValue().size(); idx++) {
          Object value = repeatedEntry.getValue().get(idx);
          String indexedColName = repeatedColNameGen.repeatedAtIndex(colName, idx);
          if (value instanceof DiqubeData)
            dataQueue.add(new Pair<>(indexedColName, (DiqubeData) value));
          else {
            // fill ColumnInfo just before adding the values, with this we can identify the column types dynamically.
            if (colInfo.getRegisteredColumnType(indexedColName) == null)
              colInfo.registerColumnType(indexedColName, objectToColType(value));
            else if (!colInfo.getRegisteredColumnType(indexedColName).equals(objectToColType(value)))
              throw new IOException("Column '" + indexedColName + "' has at least two different data types: "
                  + colInfo.getRegisteredColumnType(indexedColName) + " <-> " + objectToColType(value)
                  + ". This is not allowed.");

            Object[] valueArray = (Object[]) Array.newInstance(value.getClass(), 1);
            valueArray[0] = value;
            columnShardBuilderManager.addValues(indexedColName, valueArray, rowId);
          }
        }

        String lengthColName = repeatedColNameGen.repeatedLength(colName);
        repeatedLengthColNames.add(lengthColName);
        columnShardBuilderManager.addValues(lengthColName, new Long[] { Long.valueOf(repeatedEntry.getValue().size()) },
            rowId);
      }
    }
  }

  private ColumnType objectToColType(Object o) throws IOException {
    if (o instanceof String)
      return ColumnType.STRING;
    if (o instanceof Long)
      return ColumnType.LONG;
    if (o instanceof Double)
      return ColumnType.DOUBLE;
    throw new IOException("Incompatible data type of " + o.toString());
  }

  private Object defaultValueByType(ColumnType colType) throws IOException {
    switch (colType) {
    case STRING:
      return LoaderColumnInfo.DEFAULT_STRING;
    case LONG:
      return LoaderColumnInfo.DEFAULT_LONG;
    case DOUBLE:
      return LoaderColumnInfo.DEFAULT_DOUBLE;
    }
    throw new IOException("Non-valid column type " + colType);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    try {
      // use columnShardBuilderManager to build all columnShards
      // this will start compressing etc. and will take some time.
      Collection<StandardColumnShard> colShards = new ArrayList<>();
      for (String colName : columnShardBuilderManager.getAllColumnsWithValues()) {
        try {
          // fill remaining rows with default value - this can happen in repeated cols where not all rows have the same
          // number of entries of the col.
          if (!repeatedLengthColNames.contains(colName))
            columnShardBuilderManager.fillEmptyRowsWithValue(colName,
                defaultValueByType(colInfo.getFinalColumnType(colName)));
          else
            // fill "length" columns with "0" instead of the default long (which would be -1).
            columnShardBuilderManager.fillEmptyRowsWithValue(colName, 0L);

          colShards.add(columnShardBuilderManager.build(colName));
        } catch (Exception e) {
          throw new IOException("Could not build column '" + colName + "'", e);
        }
      }

      TableFactory tableFactory = ctx.getBean(TableFactory.class);
      TableShard tableShard = tableFactory.createTableShard(
          // this tableName will be overwritten when importing the data into a diqube-server.
          "hadoop_created", colShards);

      // serialize that data into the new file.
      DataSerializationManager dataSerializationManager = ctx.getBean(DataSerializationManager.class);
      DataSerializer serializer = dataSerializationManager.createSerializer(TableShard.class);
      try {
        serializer.serialize(tableShard, outStream, new ObjectDoneConsumer() {
          @Override
          public void accept(DataSerialization<?> t) {
            // set all properties to null after an object has been fully serialized. This will enabled the GC to clean
            // up some stuff.
            NullUtil.setAllPropertiesToNull(t, null /* swallow all exceptions */);
          }
        });
      } catch (SerializationException e) {
        throw new IOException("Could not serialize data: " + e.getMessage(), e);
      }
      outStream.flush();
    } finally {
      ctx.close();
      outStream.close();
    }
  }

}
