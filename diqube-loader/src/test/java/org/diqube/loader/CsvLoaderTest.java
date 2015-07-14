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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.dict.DoubleDictionary;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.data.str.dict.StringDictionary;
import org.diqube.loader.columnshard.ColumnShardBuilder;
import org.diqube.util.BigByteBuffer;
import org.diqube.util.IoUtils;
import org.diqube.util.PrimitiveUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the {@link CsvLoader}.
 *
 * @author Bastian Gloeckle
 */
public class CsvLoaderTest {

  private static final String CSV_SIMPLE_CLASSPATH = "/CsvLoaderTestSimple.csv";
  private static final String CSV_COL_A = "colA";
  private static final String CSV_COL_B = "colB";
  private static final String CSV_COL_C = "colC";

  private CsvLoader csvLoader;
  private LoaderColumnInfo colInfo;

  private AnnotationConfigApplicationContext dataContext;

  @BeforeMethod
  public void setUp() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    csvLoader = dataContext.getBean(CsvLoader.class);
    colInfo = new LoaderColumnInfo(ColumnType.STRING);
  }

  @AfterMethod
  public void shutDown() {
    dataContext.close();
  }

  @Test
  public void smallSimpleCsv() throws LoadException {
    // GIVEN
    // simple CSV with 3 columns, one mapped to String, one Long and one Double.
    BigByteBuffer buf = IoUtils.inputStreamToBigByteBuffer(getClass().getResourceAsStream(CSV_SIMPLE_CLASSPATH));
    colInfo.registerColumnType(CSV_COL_B, ColumnType.LONG);
    colInfo.registerColumnType(CSV_COL_C, ColumnType.DOUBLE);

    // WHEN
    // loading table
    TableShard table = csvLoader.load(0L, buf, "Test", colInfo);

    // THEN
    Assert.assertEquals(table.getNumberOfRowsInShard(), 4, "Expected 4 rows");
    Assert.assertNotNull(table.getStringColumns().get(CSV_COL_A), "Expected String column");
    Assert.assertNotNull(table.getLongColumns().get(CSV_COL_B), "Expected Long column");
    Assert.assertNotNull(table.getDoubleColumns().get(CSV_COL_C), "Expected Double column");

    @SuppressWarnings("unchecked")
    List<String> colAValues = resolveAllValues(table.getStringColumns().get(CSV_COL_A));
    @SuppressWarnings("unchecked")
    List<Long> colBValues = resolveAllValues(table.getLongColumns().get(CSV_COL_B));
    @SuppressWarnings("unchecked")
    List<Double> colCValues = resolveAllValues(table.getDoubleColumns().get(CSV_COL_C));

    Assert.assertEquals(new HashSet<String>(colAValues).size(), colAValues.size(),
        "Duplicate values in column not expected");
    Assert.assertEquals(new HashSet<Long>(colBValues).size(), colBValues.size(),
        "Duplicate values in column not expected");
    Assert.assertEquals(new HashSet<Double>(colCValues).size(), colCValues.size(),
        "Duplicate values in column not expected");

    Assert.assertEquals(new HashSet<String>(colAValues),
        new HashSet<String>(Arrays.asList(new String[] { "1", "2", "3", "4" })),
        "Different values expected (inspect data type!)");
    Assert.assertEquals(new HashSet<Long>(colBValues), new HashSet<Long>(Arrays.asList(new Long[] { 1L, 2L, 3L, 4L })),
        "Different values expected (inspect data type!)");
    Assert.assertEquals(new HashSet<Double>(colCValues),
        new HashSet<Double>(Arrays.asList(new Double[] { 1., 2., 3., 4. })),
        "Different values expected (inspect data type!)");
  }

  @Test
  public void longCsvTestProposalRows() throws LoadException {
    // GIVEN
    // a CSV with PROPOSAL_ROWS rows with numbers 0..PROPOSAL_ROWS-1
    int rows = ColumnShardBuilder.PROPOSAL_ROWS;
    BigByteBuffer buf = generateCsvOneColumn(CSV_COL_A, rows);
    colInfo.registerColumnType(CSV_COL_A, ColumnType.LONG);

    // WHEN
    // parsing this
    TableShard shard = csvLoader.load(0L, buf, "Test", colInfo);

    // THEN
    Assert.assertEquals(shard.getNumberOfRowsInShard(), rows, "Expected " + rows + " rows");

    @SuppressWarnings("unchecked")
    List<Long> colAValues = resolveAllValues(shard.getLongColumns().get(CSV_COL_A));

    Assert.assertEquals(new HashSet<>(colAValues), generateLongSetRange(0, rows), "Expected correct values");

    Assert.assertEquals(shard.getLongColumns().get(CSV_COL_A).getPages().size(), 1, "Only one ColumnPage expected");
  }

  @Test
  public void longCsvTestProposalRowsPlusOne() throws LoadException {
    // GIVEN
    // a CSV with PROPOSAL_ROWS+1 rows with numbers 0..PROPOSAL_ROWS
    int rows = ColumnShardBuilder.PROPOSAL_ROWS + 1;
    BigByteBuffer buf = generateCsvOneColumn(CSV_COL_A, rows);
    colInfo.registerColumnType(CSV_COL_A, ColumnType.LONG);

    // WHEN
    // parsing this
    TableShard shard = csvLoader.load(0L, buf, "Test", colInfo);

    // THEN
    Assert.assertEquals(shard.getNumberOfRowsInShard(), rows, "Expected " + rows + " rows");

    @SuppressWarnings("unchecked")
    List<Long> colAValues = resolveAllValues(shard.getLongColumns().get(CSV_COL_A));

    Assert.assertEquals(new HashSet<>(colAValues), generateLongSetRange(0, rows), "Expected correct values");

    Assert.assertEquals(shard.getLongColumns().get(CSV_COL_A).getPages().size(), 2, "Two ColumnPages expected");
  }

  @Test(expectedExceptions = LoadException.class)
  public void unparsableCsv() throws LoadException {
    // GIVEN
    String csv = //
        CSV_COL_A + "," + CSV_COL_B + "\n" + //
            "1," + Long.MAX_VALUE + "9\n";

    colInfo.registerColumnType(CSV_COL_A, ColumnType.LONG);
    colInfo.registerColumnType(CSV_COL_B, ColumnType.LONG);

    // WHEN
    csvLoader.load(0L, new BigByteBuffer(csv.getBytes()), "Test", colInfo);

    // THEN: exception
  }

  /**
   * Generates a {@link BigByteBuffer} containing a specific amount of CSV rows, each row containing one of the numbers
   * 0..rows-1, each number is exactly once in the CSV.
   */
  private static BigByteBuffer generateCsvOneColumn(String colName, int rows) {
    StringBuffer sb = new StringBuffer(colName);
    sb.append('\n');
    for (int i = 0; i < rows; i++) {
      sb.append(Integer.toString(i));
      sb.append('\n');
    }
    return new BigByteBuffer(sb.toString().getBytes());
  }

  private static Set<Long> generateLongSetRange(long fromIncluded, long toExcluded) {
    Set<Long> res = new HashSet<>();
    for (long l = fromIncluded; l < toExcluded; l++)
      res.add(l);
    return res;
  }

  /**
   * Return decompressed values of all rows in given column.
   * 
   * @return List of either String, Long or Double, according to col.getColumnType().
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static List resolveAllValues(StandardColumnShard col) {
    List res = new ArrayList();
    for (ColumnPage page : ((Map<Long, ColumnPage>) col.getPages()).values()) {
      // find column value IDs from column page value Ids
      List<Long> cvIds = Arrays.asList(PrimitiveUtils.toBoxedArray(page.getValues().decompressedArray())).stream()
          .map(cpvId -> page.getColumnPageDict().decompressValue(cpvId)).collect(Collectors.toList());

      switch (col.getColumnType()) {
      case STRING:
        res.addAll(cvIds.stream().map(cvId -> ((StringDictionary) col.getColumnShardDictionary()).decompressValue(cvId))
            .collect(Collectors.toList()));
        break;
      case LONG:
        res.addAll(cvIds.stream().map(cvId -> ((LongDictionary) col.getColumnShardDictionary()).decompressValue(cvId))
            .collect(Collectors.toList()));
        break;
      case DOUBLE:
        res.addAll(cvIds.stream().map(cvId -> ((DoubleDictionary) col.getColumnShardDictionary()).decompressValue(cvId))
            .collect(Collectors.toList()));
        break;
      }
    }
    return res;
  }

  public static void main(String[] args) throws LoadException, InterruptedException {
    try (AnnotationConfigApplicationContext dataContext = new AnnotationConfigApplicationContext()) {
      dataContext.scan("org.diqube.data");
      dataContext.scan("org.diqube.loader");
      dataContext.refresh();

      CsvLoader loader = dataContext.getBean(CsvLoader.class);
      LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG);
      // colInfo.registerColumnType("serialno", ColumnType.LONG);
      colInfo.registerColumnType("RT", ColumnType.STRING);
      TableShard shard = loader.load(0L, "/home/basti/Downloads/PUMS-2005-2009/ss09husa.csv", "husa", colInfo);

      int rows = 0;
      for (ColumnPage page : shard.getStringColumns().values().iterator().next().getPages().values()) {
        rows += page.size();
      }

      System.err.println("Loaded " + rows + " rows.");
      Thread.sleep(10000);
    }
  }

}
