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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializer.ObjectDoneConsumer;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileWriter;
import org.diqube.hadoop.DiqubeRow.DiqubeData;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.name.RepeatedColumnNameGenerator;
import org.diqube.util.NullUtil;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * A {@link RecordWriter} which writes to .diqube files which contain potentially multiple table shards.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeRecordWriter extends RecordWriter<NullWritable, DiqubeRow> {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeRecordWriter.class);

  private DataOutputStream outStream;
  private AnnotationConfigApplicationContext ctx;
  private ColumnShardBuilderManager columnShardBuilderManager;
  private AtomicLong nextRowId = new AtomicLong(0L);
  private RepeatedColumnNameGenerator repeatedColNameGen;
  private LoaderColumnInfo colInfo;
  private Set<String> repeatedLengthColNames = new ConcurrentSkipListSet<>();
  private String fileComment;
  private AtomicLong numberOfRowsInCurrentColShardBuilders = new AtomicLong(0);

  private long memoryCheckRowCount;
  private long memoryFlushMb;

  /** Writer of diqube file. Will be initialized lazily. */
  private DiqubeFileWriter diqubeFileWriter = null;

  private Supplier<ColumnShardBuilderManager> columShardBuilderManagerFactoryFunction;

  public DiqubeRecordWriter(DataOutputStream outStream, String fileComment, long memoryCheckRowCount,
      long memoryFlushMb) {
    this.outStream = outStream;
    this.fileComment = fileComment;
    this.memoryCheckRowCount = memoryCheckRowCount;
    this.memoryFlushMb = memoryFlushMb;

    ctx = new AnnotationConfigApplicationContext();
    // do not enable newDataWatcher and/or Config.
    ctx.scan("org.diqube");
    ctx.refresh();

    colInfo = new LoaderColumnInfo(ColumnType.LONG);

    columShardBuilderManagerFactoryFunction =
        () -> ctx.getBean(ColumnShardBuilderFactory.class).createColumnShardBuilderManager(colInfo, nextRowId.get());

    columnShardBuilderManager = columShardBuilderManagerFactoryFunction.get();
    repeatedColNameGen = ctx.getBean(RepeatedColumnNameGenerator.class);
  }

  @Override
  public void write(NullWritable key, DiqubeRow row) throws IOException, InterruptedException {
    if (row.getData() == null)
      throw new IOException("No data specified.");

    try {
      row.getData().validate();

      if (row.getData().isEmpty())
        // root object must not be empty, otherwise the row would be empty. This is most probably an error!
        throw new IllegalStateException("Root DiqubeData object is empty.");
    } catch (IllegalStateException e) {
      throw new IOException("DiqubeRow not valid: " + e.getMessage(), e);
    }

    // fill in data of that single new row into columnShardBuilderManager
    long rowId = nextRowId.getAndIncrement();
    addRowToColumnShardBuilderManager(row, rowId);
    long numberOfRowsInCurShard = numberOfRowsInCurrentColShardBuilders.incrementAndGet();
    if (numberOfRowsInCurShard % memoryCheckRowCount == 0) {
      long memoryConsumptionBytes = columnShardBuilderManager.calculateApproximateSizeInBytes();
      logger.info("Current approximate memory consumption: {} MB", memoryConsumptionBytes / (1024 * 1024));
      if (memoryConsumptionBytes / (1024 * 1024) >= memoryFlushMb) {
        try {
          flushNewTableShard();
        } catch (Throwable t) {
          logger.error("Could not flush new table shard", t);
          throw new IOException("Could not flush new table shard", t);
        }
      }
    }
  }

  /**
   * Add the values of a {@link DiqubeRow} to {@link #columnShardBuilderManager}.
   */
  private void addRowToColumnShardBuilderManager(DiqubeRow row, long rowId) throws IOException {
    Deque<Pair<String, DiqubeData>> dataQueue = new LinkedList<>();
    dataQueue.add(new Pair<>("", row.getData()));
    while (!dataQueue.isEmpty()) {
      Pair<String, DiqubeData> dataPair = dataQueue.poll();
      DiqubeData data = dataPair.getRight();
      String parentColName = dataPair.getLeft();

      if (data.isEmpty())
        // skip empty DiqubeData objects.
        continue;

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

  /**
   * Create a new {@link TableShard} from the data available in {@link #columnShardBuilderManager} and serialize it to
   * the output stream.
   * 
   * Resets the {@link #columnShardBuilderManager} to be ready for a new TableShard afterwards.
   */
  private void flushNewTableShard() throws IOException {
    if (diqubeFileWriter == null) {
      logger.info("Initializing new DiqubeFileWriter...");
      DiqubeFileFactory fileFactory = ctx.getBean(DiqubeFileFactory.class);
      diqubeFileWriter = fileFactory.createDiqubeFileWriter(outStream);
      diqubeFileWriter.setComment(fileComment);
    }

    logger.info(
        "Creating new TableShard and flushing data to output stream "
            + "(up to rowId {}, first rowId is {}, number of rows {}, number of columns {})...",
        nextRowId.get() - 1, columnShardBuilderManager.getFirstRowIdInShard(),
        numberOfRowsInCurrentColShardBuilders.get(), columnShardBuilderManager.getAllColumnsWithValues().size());

    // use columnShardBuilderManager to build all columnShards
    // this will start compressing etc. and will take some time.
    Collection<StandardColumnShard> colShards = new ArrayList<>();
    for (String colName : columnShardBuilderManager.getAllColumnsWithValues()) {
      try {
        // fill remaining rows with default value - this can happen in repeated cols where not all rows have the same
        // number of entries of the col.
        if (repeatedLengthColNames.contains(colName))
          // fill "length" columns with "0" instead of the default long (which would be -1).
          columnShardBuilderManager.fillEmptyRowsWithValue(colName, 0L);

        logger.info("Building column {}", colName);
        StandardColumnShard newShard = columnShardBuilderManager.buildAndFree(colName);
        if (newShard == null)
          throw new IOException("Could not build column '" + colName + "', the result was null.");

        colShards.add(newShard);
      } catch (Exception e) {
        throw new IOException("Could not build column '" + colName + "'", e);
      }
    }

    logger.info("Columns created, preparing to write new TableShard to output..");

    TableFactory tableFactory = ctx.getBean(TableFactory.class);
    TableShard tableShard = tableFactory.createDefaultTableShard(
        // this tableName will be overwritten when importing the data into a diqube-server.
        "hadoop_created", colShards);

    try {
      Exception[] serializeException = new Exception[1];
      serializeException[0] = null;
      diqubeFileWriter.writeTableShard(tableShard, new ObjectDoneConsumer() {
        @Override
        public void accept(DataSerialization<?> t) {
          // set all properties to null after an object has been fully serialized. This will enabled the GC to clean
          // up some stuff.
          NullUtil.setAllPropertiesToNull(t, new BiConsumer<String, Exception>() {
            @Override
            public void accept(String t, Exception u) {
              serializeException[0] = new RuntimeException("Error while nulling field '" + t + "'", u);
            }
          });
        }
      });
      if (serializeException[0] != null)
        throw new IOException(serializeException[0]);
    } catch (SerializationException e) {
      throw new IOException("Could not serialize/write to output stream.", e);
    }
    logger.info("Data for new TableShard written successfully.");

    // create a new ColShardBuildManager to build the next shard
    columnShardBuilderManager = columShardBuilderManagerFactoryFunction.get();
    numberOfRowsInCurrentColShardBuilders.set(0L);
    System.gc(); // hint to the system that there might be memory to free up.
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

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    logger.info("Closing record writer...");
    try {
      flushNewTableShard();
      outStream.flush();
    } catch (Throwable t) {
      logger.error("Could not flush last TableShard of writer that was closed.", t);
      throw new IOException("Could not flush last TableShard of writer that was closed.", t);
    } finally {
      if (diqubeFileWriter != null) {
        try {
          diqubeFileWriter.close();
        } catch (Throwable t) {
          logger.error("Could not close diqubeFileWriter", t);
          throw new IOException("Could not close diqubeFileWriter", t);
        }
      }
      logger.info("All data has been written to the output stream successfully.");
      ctx.close();
      outStream.close();
    }
  }
}
