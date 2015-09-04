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

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.ColumnType;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.util.RepeatedColumnNameGenerator;
import org.diqube.loader.JsonLoader.Parser.Handler;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.loader.util.ParallelLoadAndTransposeHelper;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.BigByteBuffer;
import org.diqube.util.HashingBatchCollector;
import org.diqube.util.exception.WrappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * {@link Loader} which loads data from JSON files.
 * 
 * <p>
 * This Loader can load hierarchical data.
 * 
 * <p>
 * The JSON file must consist of an array, which contains a complex object for each logical "row" in the resulting
 * table. The complex object can in turn contain other complex objects (hierarchy) and can contain arrays itself
 * (repeated fields). Currently, though, each top-level object needs to recursively be made up of the exactly same
 * fields ("optional" fields are not supported yet).
 * 
 * <p>
 * This loader will return only one TableShard for a whole JSON input file.
 * 
 * TODO #14 support optional fields.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class JsonLoader implements Loader {
  public static final int BUCKET_SIZE = 1_000;

  private static final Logger logger = LoggerFactory.getLogger(JsonLoader.class);

  @Inject
  private ColumnShardBuilderFactory columnShardBuilderManagerFactory;

  @Inject
  private TableFactory tableFactory;

  @Inject
  private ExecutorManager executorManager;

  @Inject
  private RepeatedColumnNameGenerator repeatedColNames;

  @Override
  public Collection<TableShard> load(long firstRowId, String filename, String tableName, LoaderColumnInfo columnInfo)
      throws LoadException {

    logger.info("Reading data for table '{}' from '{}'.", new Object[] { tableName, filename });

    try (RandomAccessFile f = new RandomAccessFile(filename, "r")) {
      BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load());

      return load(firstRowId, buf, tableName, columnInfo);
    } catch (IOException e) {
      throw new LoadException("Could not load " + filename, e);
    }
  }

  @Override
  public Collection<TableShard> load(long firstRowId, BigByteBuffer jsonBuffer, String tableName,
      LoaderColumnInfo columnInfo) throws LoadException {
    JsonFactory factory = new JsonFactory();

    // parse the jsonBuffer and identify all columns, their types and repeated columns.
    Map<String, ColumnType> columnTypes = new HashMap<>();
    Set<String> repeatedCols = new HashSet<>();
    NavigableMap<Long, Long> objectLocations = new TreeMap<>();
    logger.info("Inspecting JSON in order to find all columns...");
    findColumnInfo(factory, jsonBuffer, columnTypes, repeatedCols, objectLocations);

    // TODO #15 validate that we did not identify different things throughout the same input file.

    // put colTypes into columnInfo, throw exception if something different was identified than what was specified.
    for (String colName : columnTypes.keySet()) {
      // TODO #15 introduce import data specificity - when default is "double" but "long" identified, make it "double"
      if (columnInfo.isDefaultDataType(colName))
        columnInfo.registerColumnType(colName, columnTypes.get(colName));
      else if (!columnInfo.getFinalColumnType(colName).equals(ColumnType.STRING) // overriding to STRING type is allowed
          && !columnInfo.getFinalColumnType(colName).equals(columnTypes.get(colName)))
        throw new LoadException(
            "Column '" + colName + "': Automatically identified type to be " + columnTypes.get(colName) + ", but "
                + columnInfo.getFinalColumnType(colName) + " was specified. This is invalid.");
    }

    logger.info("Found {} columns.", columnTypes.size());

    Stream<Parser> stream = StreamSupport.stream(
        new JsonSpliterator(objectLocations, //
            factory, //
            repeatedColNames, //
            jsonBuffer, //
            objectLocations.firstKey(), //
            objectLocations.lastEntry().getValue() + 1), //
        true);

    ColumnShardBuilderManager columnBuilderManager =
        columnShardBuilderManagerFactory.createColumnShardBuilderManager(columnInfo, firstRowId);

    String[] colNames = columnTypes.keySet().stream().toArray(l -> new String[l]);
    Map<String, Integer> colToColIndex = new HashMap<>();
    for (int i = 0; i < colNames.length; i++)
      colToColIndex.put(colNames[i], i);

    ParallelLoadAndTransposeHelper transposer =
        new ParallelLoadAndTransposeHelper(executorManager, columnInfo, columnBuilderManager, colNames, tableName);

    logger.info("Loading data and transforming to temporary columnar representation...");

    try {
      transposer.transpose(firstRowId, new Consumer<ConcurrentLinkedDeque<String[][]>>() {
        @Override
        public void accept(ConcurrentLinkedDeque<String[][]> rowWiseTarget) {
          stream.parallel().map(new Function<Parser, String[]>() {
            @Override
            public String[] apply(Parser parser) {
              try {
                return parseOneEntry(parser, colNames, repeatedCols, colToColIndex);
              } catch (LoadException e) {
                throw new WrappingException(e);
              }
            }
          }).collect(new HashingBatchCollector<String[]>(BUCKET_SIZE, //
              (len) -> new String[len][], //
              a -> rowWiseTarget.add(a)) //
          );
        }
      });
    } catch (WrappingException e) {
      LoadException loadEx = (LoadException) e.getWrappedException();
      throw loadEx;
    }

    logger.info("Read data for table {}. Compressing and creating final representation...", tableName);

    // Build the columns.
    List<StandardColumnShard> columns = new LinkedList<>();
    for (String colName : columnBuilderManager.getAllColumnsWithValues()) {
      StandardColumnShard columnShard = columnBuilderManager.buildAndFree(colName);

      columns.add(columnShard);
    }

    logger.info("Columns for new table shard of table {} created, creating TableShard...", tableName);
    TableShard tableShard = tableFactory.createTableShard(tableName, columns);

    logger.info(
        "Table shard for new table shard of table {} created successfully, it contains {} rows starting from rowId {}",
        tableName, tableShard.getNumberOfRowsInShard(), tableShard.getLowestRowId());
    return Arrays.asList(tableShard);
  }

  /**
   * Parses one top level object in the JSON and produces a corresponding String[].
   * 
   * TODO support other than String[].
   * 
   * @param parser
   *          The {@link Parser} that is prepared to load the top level input object.
   * @param colNames
   *          Names of the columns. The result of this method will be an array where the index in the array denotes the
   *          column for which the entry contains the value - It is the same ordering as this colNames parameter.
   * @param repeatedCols
   *          Set of repeated columns.
   * @param colToColIndex
   *          Map from column name to the index in colNames.
   * @return String[] containing the values of the object for each column. Ordering is the same as colNames.
   */
  private String[] parseOneEntry(Parser parser, String[] colNames, Set<String> repeatedCols,
      Map<String, Integer> colToColIndex) throws LoadException {
    String[] res = new String[colNames.length];
    parser.parse(new Handler() {
      @Override
      public boolean isArray(String colName) {
        return repeatedCols.contains(colName);
      }

      @Override
      public void valueString(String colName, JsonParser parser) throws LoadException {
        try {
          res[colToColIndex.get(colName)] = parser.getValueAsString().intern();
        } catch (IOException e) {
          throw new LoadException("Could not parse value of column " + colName + ": " + e.getMessage(), e);
        }
      }

      @Override
      public void valueLong(String colName, JsonParser parser) throws LoadException {
        try {
          res[colToColIndex.get(colName)] = Long.valueOf(parser.getValueAsLong()).toString().intern();
        } catch (IOException e) {
          throw new LoadException("Could not parse value of column " + colName + ": " + e.getMessage(), e);
        }
      }

      @Override
      public void valueDouble(String colName, JsonParser parser) throws LoadException {
        try {
          res[colToColIndex.get(colName)] = Double.valueOf(parser.getValueAsDouble()).toString().intern();
        } catch (IOException e) {
          throw new LoadException("Could not parse value of column " + colName + ": " + e.getMessage(), e);
        }
      }

      @Override
      public void endArray(String colName, int length) throws LoadException {
        String lengthCol = repeatedColNames.repeatedLength(colName);
        res[colToColIndex.get(lengthCol)] = Integer.toString(length);
      }
    });
    return res;
  }

  /**
   * Parse the jsonBuffer and find information about all the columns used in the input.
   * 
   * @param factory
   *          The factory which can be passed to {@link Parser}.
   * @param jsonBuffer
   *          The buffer containing the raw JSON.
   * @param resColumnTypes
   *          Result (this object will be filled!): The data type for all found columns (key: column name).
   * @param resRepeatedCols
   *          Result (this object will be filled!): The column names of the columns which are repeated (=arrays).
   * @param resObjectPositions
   *          Result (this object will be filled!): A Map that identifies the byte-locations of top level objects in the
   *          input stream. The key is the index a top level object begins, the value is where it ends. As the
   *          "top level objects" (=those that will be used as rows for our new table) are contained in a standard JSON
   *          array and there might be some whitespace between the objects, there might be gaps. When cutting the
   *          jsonBuffer according to these values, each chunk will be parseable as a single top level object (= a
   *          single row in the new table).
   */
  private void findColumnInfo(JsonFactory factory, BigByteBuffer jsonBuffer, Map<String, ColumnType> resColumnTypes,
      Set<String> resRepeatedCols, NavigableMap<Long, Long> resObjectPositions) throws LoadException {
    new Parser(factory, repeatedColNames, jsonBuffer.createInputStream()).parse(new Handler() {
      @Override
      public boolean isArray(String colName) {
        return resRepeatedCols.contains(colName);
      }

      @Override
      public void startArray(String colName) {
        resRepeatedCols.add(colName);
        // length column.
        resColumnTypes.put(repeatedColNames.repeatedLength(colName), ColumnType.LONG);
      }

      @Override
      public void topLevelObjectStart(Long pos) {
        resObjectPositions.put(pos, null);
      }

      @Override
      public void topLevelObjectEnd(Long pos) {
        resObjectPositions.put(resObjectPositions.lastKey(), pos);
      }

      @Override
      public void valueString(String colName, JsonParser parser) {
        resColumnTypes.put(colName, ColumnType.STRING);
      }

      @Override
      public void valueLong(String colName, JsonParser parser) {
        if (!resColumnTypes.containsKey(colName)) // ignore LONG if there is a DOUBLE already.
          resColumnTypes.put(colName, ColumnType.LONG);
      }

      @Override
      public void valueDouble(String colName, JsonParser parser) {
        resColumnTypes.put(colName, ColumnType.DOUBLE);
      }
    });
  }

  /**
   * Encapsulates parsing of a JSON and creation of valid column names. A {@link Handler} is called at interesting
   * places to do actual work.
   */
  public static class Parser {
    private JsonFactory factory;
    private InputStream jsonStream;
    private RepeatedColumnNameGenerator repeatedColNames;

    public Parser(JsonFactory factory, RepeatedColumnNameGenerator repeatedColNames, InputStream jsonStream) {
      this.factory = factory;
      this.repeatedColNames = repeatedColNames;
      this.jsonStream = jsonStream;
    }

    /**
     * Parse the JSON stream, create correct column names and call the {@link Handler} to handle those.
     */
    public void parse(Handler handler) throws LoadException {
      JsonParser parser = null;
      try {
        parser = factory.createParser(jsonStream);

        Deque<String> colNameStack = new LinkedList<>();
        boolean directlyInArray = false;
        Deque<Integer> nextArrayIndexStack = new LinkedList<>();

        JsonToken token;
        while ((token = parser.nextToken()) != null) {
          switch (token) {
          case START_ARRAY:
            if (directlyInArray)
              throw new LoadException("Cannot load JSON because it contains a multi-dimensional array.");
            if (!colNameStack.isEmpty()) {
              directlyInArray = true;
              nextArrayIndexStack.add(0); // first index in array is 0
              handler.startArray(colNameStack.getLast());
            }
            break;
          case END_ARRAY:
            Integer length = nextArrayIndexStack.pollLast();
            directlyInArray = false; // no multi-dimensional arrays, therefore we are not in an array any more.
            String arrayName = colNameStack.pollLast(); // was pushed by FIELD_NAME. in the non-array case this is
                                                        // polled by VALUE_*.
            if (arrayName != null && !arrayName.equals(""))
              handler.endArray(arrayName, length);
            break;
          case START_OBJECT:
            if (directlyInArray) {
              int idx = nextArrayIndexStack.pollLast();
              String colName = repeatedColNames.repeatedAtIndex(colNameStack.getLast(), idx);
              colNameStack.add(colName);
              nextArrayIndexStack.add(idx + 1);
              directlyInArray = false;
            }
            if (colNameStack.isEmpty())
              handler.topLevelObjectStart(parser.getCurrentLocation().getByteOffset() - 1);
            break;
          case END_OBJECT:
            if (colNameStack.isEmpty())
              handler.topLevelObjectEnd(parser.getCurrentLocation().getByteOffset());
            else if (colNameStack.size() >= 2) {
              // peek to the element before the last in colNameStack - if that is an array, then we need to poll away
              // the element that we added in START_OBJECT.
              Iterator<String> it = colNameStack.descendingIterator();
              it.next();
              if (handler.isArray(it.next())) {
                directlyInArray = true;
                colNameStack.pollLast();
              }
            }
            break;
          case FIELD_NAME:
            if (colNameStack.isEmpty())
              colNameStack.add(parser.getText());
            else
              colNameStack.add(colNameStack.getLast() + "." + parser.getText());
            break;
          case VALUE_STRING:
            if (colNameStack.isEmpty())
              throw new LoadException("Ensure that the outer array contains JSON objects and not values directly.");
            if (directlyInArray) {
              int idx = nextArrayIndexStack.pollLast();
              String colName = repeatedColNames.repeatedAtIndex(colNameStack.getLast(), idx);
              handler.valueString(colName, parser);
              nextArrayIndexStack.add(idx + 1);
            } else
              handler.valueString(colNameStack.pollLast(), parser);
            break;
          case VALUE_NUMBER_INT:
            if (colNameStack.isEmpty())
              throw new LoadException("Ensure that the outer array contains JSON objects and not values directly.");
            if (directlyInArray) {
              int idx = nextArrayIndexStack.pollLast();
              String colName = repeatedColNames.repeatedAtIndex(colNameStack.getLast(), idx);
              handler.valueLong(colName, parser);
              nextArrayIndexStack.add(idx + 1);
            } else
              handler.valueLong(colNameStack.pollLast(), parser);
            break;
          case VALUE_NUMBER_FLOAT:
            if (colNameStack.isEmpty())
              throw new LoadException("Ensure that the outer array contains JSON objects and not values directly.");
            if (directlyInArray) {
              int idx = nextArrayIndexStack.pollLast();
              String colName = repeatedColNames.repeatedAtIndex(colNameStack.getLast(), idx);
              handler.valueDouble(colName, parser);
              nextArrayIndexStack.add(idx + 1);
            } else
              handler.valueDouble(colNameStack.pollLast(), parser);
            break;
          case VALUE_TRUE:
          case VALUE_FALSE:
            if (colNameStack.isEmpty())
              throw new LoadException("Ensure that the outer array contains JSON objects and not values directly.");
            if (directlyInArray) {
              int idx = nextArrayIndexStack.pollLast();
              String colName = repeatedColNames.repeatedAtIndex(colNameStack.getLast(), idx);
              handler.valueLong(colName, parser);
              nextArrayIndexStack.add(idx + 1);
            } else
              handler.valueLong(colNameStack.pollLast(), parser);
            break;
          case VALUE_NULL:
            // TODO #14 support null
            break;
          case NOT_AVAILABLE:
          case VALUE_EMBEDDED_OBJECT:
            // noop.
            break;
          }
        }
      } catch (IOException e) {
        throw new LoadException("Could not parse column names from JSON: " + e.getMessage(), e);
      } finally {
        if (parser != null)
          try {
            parser.close();
          } catch (IOException e) {
            // swallow.
          }
      }
    }

    /**
     * Externally specified Handler for handling events while parsing.
     */
    public static abstract class Handler {
      /**
       * Has to return <code>true</code> if a colName is specified which is known to be a repeatable field/array.
       * 
       * If the column is actually repeatable, then this method will be called after corresponding calls to
       * {@link #startArray(String)}.
       */
      abstract public boolean isArray(String colName) throws LoadException;

      /**
       * The start of an array with the given colName was parsed.
       * 
       * This will not be called for the outermost array, as that isn't of interest to us.
       */
      public void startArray(String colName) throws LoadException {
      }

      /**
       * The end of an array including the number of elements that were identified.
       */
      public void endArray(String colName, int length) throws LoadException {
      }

      /**
       * A new top level object starts at the given byte index.
       */
      public void topLevelObjectStart(Long pos) throws LoadException {
      }

      /**
       * A new top level object ends at the given byte index. This will be called after the corresponding call to
       * {@link #topLevelObjectStart(Long)} - there won't be any intermediary calls to
       * {@link #topLevelObjectStart(Long)}.
       */
      public void topLevelObjectEnd(Long pos) throws LoadException {
      }

      /**
       * A String type value column has been found. If one needs to resolve the actual value, use the provided
       * {@link JsonParser}.
       */
      public void valueString(String colName, JsonParser parser) throws LoadException {
      }

      /**
       * A Long type value column has been found. If one needs to resolve the actual value, use the provided
       * {@link JsonParser}.
       */
      public void valueLong(String colName, JsonParser parser) throws LoadException {
      }

      /**
       * A Double type value column has been found. If one needs to resolve the actual value, use the provided
       * {@link JsonParser}.
       */
      public void valueDouble(String colName, JsonParser parser) throws LoadException {
      }
    }
  }

  /**
   * A {@link Spliterator} on a JSON input which can be used to construct a parallel {@link Stream} on an input JSON.
   * 
   * <p>
   * This spliterator is based on the result of
   * {@link JsonLoader#findColumnInfo(JsonFactory, ByteBuffer, Map, Set, NavigableMap)} -> the object location map.
   */
  public static class JsonSpliterator implements Spliterator<Parser> {

    private NavigableMap<Long, Long> objectLocations;
    private Long startPosInclusive;
    private Long endPosExclusive;
    private JsonFactory factory;
    private BigByteBuffer jsonBuffer;
    private RepeatedColumnNameGenerator repeatedColNames;

    /**
     * 
     * @param objectLocations
     *          The object locations as identified as output of
     *          {@link JsonLoader#findColumnInfo(JsonFactory, ByteBuffer, Map, Set, NavigableMap)}. Each Entry in the
     *          map specifies the byte-indices of one top level object (which will be parsed into one table row). The
     *          key of the entry specifieds the first byte index in the JSON buffer of the object, the value specifies
     *          the last index.
     * @param factory
     *          The {@link JsonFactory} that can be used by the {@link Parser}.
     * @param jsonBuffer
     *          The raw JSON input.
     * @param startPosInclusive
     *          First index of a valid top level object in the buffer that this spliterator should cover (e.g. the
     *          firstKey() of objectLocations).
     * @param endPosExclusive
     *          An index in the buffer that is the first one that this spliterator should not cover any more.
     */
    public JsonSpliterator(NavigableMap<Long, Long> objectLocations, JsonFactory factory,
        RepeatedColumnNameGenerator repeatedColNames, BigByteBuffer jsonBuffer, Long startPosInclusive,
        Long endPosExclusive) {
      this.objectLocations = objectLocations;
      this.repeatedColNames = repeatedColNames;
      this.startPosInclusive = startPosInclusive;
      this.endPosExclusive = endPosExclusive;
      this.factory = factory;
      this.jsonBuffer = jsonBuffer;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Parser> action) {
      if (startPosInclusive + 1 >= endPosExclusive)
        return false;

      long topLevelObjectStart = startPosInclusive;
      long topLevelObjectEnd = objectLocations.get(startPosInclusive);

      action.accept(new Parser(factory, repeatedColNames,
          jsonBuffer.createPartialInputStream(topLevelObjectStart, topLevelObjectEnd)));

      startPosInclusive = objectLocations.ceilingKey(startPosInclusive + 1);
      if (startPosInclusive == null)
        startPosInclusive = endPosExclusive;

      return true;
    }

    @Override
    public Spliterator<Parser> trySplit() {
      SortedMap<Long, Long> subMap = objectLocations.subMap(startPosInclusive, endPosExclusive);

      if (subMap.size() <= 2)
        return null;

      Long middle = subMap.firstKey() + ((subMap.lastKey() - subMap.firstKey()) / 2);

      Long middleKey = objectLocations.ceilingKey(middle);
      if (middleKey == null || middleKey >= endPosExclusive || middleKey == startPosInclusive)
        return null;

      if (objectLocations.subMap(middleKey, endPosExclusive).isEmpty())
        return null;

      JsonSpliterator newSplit =
          new JsonSpliterator(objectLocations, factory, repeatedColNames, jsonBuffer, middleKey, endPosExclusive);
      endPosExclusive = middleKey;

      return newSplit;
    }

    @Override
    public long estimateSize() {
      return objectLocations.subMap(startPosInclusive, endPosExclusive).size();
    }

    @Override
    public int characteristics() {
      return Spliterator.DISTINCT | Spliterator.SIZED | Spliterator.NONNULL | Spliterator.IMMUTABLE
          | Spliterator.SUBSIZED;
    }

  }
}
