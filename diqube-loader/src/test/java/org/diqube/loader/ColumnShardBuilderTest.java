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
import java.util.Set;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnShardFactory;
import org.diqube.data.dictionary.Dictionary;
import org.diqube.data.str.StringStandardColumnShard;
import org.diqube.data.str.dict.StringDictionary;
import org.diqube.loader.columnshard.ColumnShardBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for {@link ColumnShardBuilder}.
 *
 * @author Bastian Gloeckle
 */
public class ColumnShardBuilderTest {
  private static final String TEST_COL_NAME = "testCol";

  private ColumnShardBuilder<String> builder;

  private AnnotationConfigApplicationContext dataContext;

  @BeforeMethod
  public void setUp() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube.data");
    dataContext.refresh();
    builder = new ColumnShardBuilder<String>(dataContext.getBean(ColumnShardFactory.class),
        dataContext.getBean(ColumnPageFactory.class), TEST_COL_NAME, 0L);
  }

  @AfterMethod
  public void shutDown() {
    dataContext.close();
  }

  @Test
  public void buildSimpleColumnShard() {
    // GIVEN
    // simple 10-values column input, with IDs starting low
    String[] valueArray = generateStringArray(1, 10);
    builder.addValues(valueArray, 0L);

    // WHEN
    // building the shard
    ColumnShard abstractShard = builder.build();

    // THEN
    Assert.assertEquals(StringStandardColumnShard.class, abstractShard.getClass(),
        "StringStandardColumnShard expected");
    StringStandardColumnShard shard = (StringStandardColumnShard) abstractShard;

    Assert.assertEquals(shard.getName(), TEST_COL_NAME, "Correct shard name expected");
    Assert.assertEquals(shard.getPages().size(), 1, "Expected exactly one Column page");
    ColumnPage page = shard.getPages().values().iterator().next();

    Assert.assertEquals(page.getFirstRowId(), 0, "Expected first ID in page to be 1");
    Assert.assertEquals(shard.getPages().keySet().iterator().next(), (Long) page.getFirstRowId(),
        "Expected correct first ID in map from Shard to Page");
    Assert.assertEquals(resolveValues(page, shard.getColumnShardDictionary()),
        new HashSet<String>(Arrays.asList(valueArray)), "Expected correct values to be available in Column");
  }

  @Test
  public void buildLongColumnShard() {
    // GIVEN
    // simple 10-values column input, with IDs starting low
    String[] valueArray = generateStringArray(1, ColumnShardBuilder.PROPOSAL_ROWS + 1);
    builder.addValues(valueArray, 0L);

    // WHEN
    // building the shard
    ColumnShard abstractShard = builder.build();

    // THEN
    StringStandardColumnShard shard = (StringStandardColumnShard) abstractShard;
    Assert.assertEquals(shard.getPages().keySet(),
        new HashSet<Long>(Arrays.asList(new Long[] { 0L, (long) ColumnShardBuilder.PROPOSAL_ROWS })),
        "Expected two column pages at specific rowIds");

    ColumnPage page1 = shard.getPages().get(0L);
    ColumnPage page2 = shard.getPages().get((long) ColumnShardBuilder.PROPOSAL_ROWS);

    Set<String> allValues = resolveValues(page1, shard.getColumnShardDictionary());
    allValues.addAll(resolveValues(page2, shard.getColumnShardDictionary()));

    Assert.assertEquals(allValues, new HashSet<String>(Arrays.asList(valueArray)),
        "Expected all values 1..5001 to be available in Column");

    Assert.assertEquals(page1.size() + page2.size(), ColumnShardBuilder.PROPOSAL_ROWS + 1,
        "Expected correct number of values to be stored.");
  }

  @Test
  public void buildLongColumnShardReversed() {
    // GIVEN
    // simple 10-values column input, with IDs starting low
    String[] valueArray = generateStringArray(1, ColumnShardBuilder.PROPOSAL_ROWS + 1);
    for (int i = 0; i < valueArray.length / 2; i++) {
      String tmp = valueArray[i];
      valueArray[i] = valueArray[valueArray.length - 1 - i];
      valueArray[valueArray.length - 1 - i] = tmp;
    }
    builder.addValues(valueArray, 0L);

    // WHEN
    // building the shard
    ColumnShard abstractShard = builder.build();

    // THEN
    StringStandardColumnShard shard = (StringStandardColumnShard) abstractShard;
    Assert.assertEquals(shard.getPages().keySet(),
        new HashSet<Long>(Arrays.asList(new Long[] { 0L, (long) ColumnShardBuilder.PROPOSAL_ROWS })),
        "Expected two column pages at specific rowIds");

    ColumnPage page1 = shard.getPages().get(0L);
    ColumnPage page2 = shard.getPages().get((long) ColumnShardBuilder.PROPOSAL_ROWS);

    Set<String> allValues = resolveValues(page1, shard.getColumnShardDictionary());
    allValues.addAll(resolveValues(page2, shard.getColumnShardDictionary()));

    Assert.assertEquals(allValues, new HashSet<String>(Arrays.asList(valueArray)),
        "Expected all values 1..5001 to be available in Column");

    Assert.assertEquals(page1.size() + page2.size(), ColumnShardBuilder.PROPOSAL_ROWS + 1,
        "Expected correct number of values to be stored.");
  }

  private static Set<String> resolveValues(ColumnPage page, Dictionary<?> columnDictionary) {
    Set<String> res = new HashSet<String>();
    StringDictionary<?> dict = (StringDictionary<?>) columnDictionary;
    for (long pageValueId : page.getValues().decompressedArray()) {
      long columnValueId = page.getColumnPageDict().decompressValue(pageValueId);
      String value = dict.decompressValue(columnValueId);
      res.add(value);
    }
    return res;
  }

  private static String[] generateStringArray(int start, int length) {
    String[] res = new String[length];
    for (int i = 0; i < length; i++) {
      res[i] = "s" + Integer.toString(i + start);
    }
    return res;
  }
}
