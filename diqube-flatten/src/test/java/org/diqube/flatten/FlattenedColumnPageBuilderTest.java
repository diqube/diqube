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

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.LongStream;

import org.diqube.context.Profiles;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnPageFactory;
import org.diqube.data.flatten.FlattenedColumnPage;
import org.diqube.data.flatten.IndexFilteringCompressedLongArray;
import org.diqube.data.flatten.IndexRemovingCompressedLongArray;
import org.diqube.loader.columnshard.ColumnPageBuilder;
import org.diqube.util.DiqubeCollectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author Bastian Gloeckle
 */
public class FlattenedColumnPageBuilderTest {
  private static final Logger logger = LoggerFactory.getLogger(FlattenedColumnPageBuilderTest.class);

  private static final long[] decompressedValues = new long[] { 10L, 12L, 14L, 16L, 18L, 20L, 22L, 24L, 26L };

  private AnnotationConfigApplicationContext dataContext;

  private FlattenedColumnPageBuilderFactory factory;

  private ColumnPage delegate;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    factory = dataContext.getBean(FlattenedColumnPageBuilderFactory.class);

    ColumnPageBuilder colPageBuilder = new ColumnPageBuilder(dataContext.getBean(ColumnPageFactory.class));

    long[] valueIds = new long[decompressedValues.length];
    NavigableMap<Long, Long> valueToId = new TreeMap<>();
    for (int i = 0; i < decompressedValues.length; i++) {
      valueIds[i] = i;
      valueToId.put(decompressedValues[i], (long) i);
    }

    logger.info("Building delegate page...");
    delegate = colPageBuilder.withColumnPageName("delegate").withFirstRowId(0L).withValues(valueIds)
        .withValueMap(valueToId).build();
    logger.info("Delegate page built.");
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test
  public void removeNone() {
    // GIVEN
    NavigableSet<Long> notAvailableRowIds = new TreeSet<>();

    // WHEN
    ColumnPage flattenedPage = factory.createFlattenedColumnPageBuilder().withColName("1").withDelegate(delegate)
        .withFirstRowId(0L).withNotAvailableRowIds(notAvailableRowIds).build();

    // THEN
    Assert.assertNotNull(flattenedPage, "Expected to get a page returned.");

    long[] pageValueIds = flattenedPage.getValues().decompressedArray();
    Long[] pageValueIds2 = LongStream.of(pageValueIds).mapToObj(Long::valueOf).toArray(l -> new Long[l]);
    Long[] values = flattenedPage.getColumnPageDict().decompressValues(pageValueIds2);
    Assert.assertEquals(values, decompressedValues, "Expected correct values from decompressedArray.");

    for (int idx : Arrays.asList(0, decompressedValues.length / 2, decompressedValues.length - 1)) {
      long pageValueId = flattenedPage.getValues().get(idx);
      long value = flattenedPage.getColumnPageDict().decompressValue(pageValueId);

      Assert.assertEquals(value, decompressedValues[idx], "Expected correct value at index " + idx);
    }

    List<Long> multipleValueIds = flattenedPage.getValues()
        .getMultiple(Arrays.asList(0, decompressedValues.length / 2, decompressedValues.length - 1));
    Long[] multipleValues =
        flattenedPage.getColumnPageDict().decompressValues(multipleValueIds.toArray(new Long[multipleValueIds.size()]));

    Assert.assertEquals(
        Arrays.asList(multipleValues), Arrays.asList(decompressedValues[0],
            decompressedValues[decompressedValues.length / 2], decompressedValues[decompressedValues.length - 1]),
        "Expected correct results from getMultiple.");
  }

  @Test
  public void removeAll() {
    // GIVEN
    NavigableSet<Long> notAvailableRowIds = LongStream.range(0L, decompressedValues.length).mapToObj(Long::valueOf)
        .collect(DiqubeCollectors.toNavigableSet());

    // WHEN
    ColumnPage flattenedPage = factory.createFlattenedColumnPageBuilder().withColName("1").withDelegate(delegate)
        .withFirstRowId(0L).withNotAvailableRowIds(notAvailableRowIds).build();

    // THEN
    Assert.assertNull(flattenedPage, "Expected to NOT get a page returned.");
  }

  @Test
  public void removeOne() {
    // GIVEN
    NavigableSet<Long> notAvailableRowIds = new TreeSet<>(Arrays.asList(decompressedValues.length - 1L));

    // WHEN
    ColumnPage flattenedPage = factory.createFlattenedColumnPageBuilder().withColName("1").withDelegate(delegate)
        .withFirstRowId(0L).withNotAvailableRowIds(notAvailableRowIds).build();

    // THEN
    // assert type of returned page to make sure we have a test for each type.
    Assert.assertTrue(flattenedPage instanceof FlattenedColumnPage, "Did expect a FlattenedColPage.");
    Assert.assertTrue(flattenedPage.getValues() instanceof IndexRemovingCompressedLongArray,
        "Did expect a IndexRemovingCompressedLongArray");

    Assert.assertNotNull(flattenedPage, "Expected to get a page returned.");

    long[] pageValueIds = flattenedPage.getValues().decompressedArray();
    Long[] pageValueIds2 = LongStream.of(pageValueIds).mapToObj(Long::valueOf).toArray(l -> new Long[l]);
    Long[] values = flattenedPage.getColumnPageDict().decompressValues(pageValueIds2);
    long[] expected = new long[decompressedValues.length - 1];
    System.arraycopy(decompressedValues, 0, expected, 0, expected.length);
    Assert.assertEquals(values, expected, "Expected correct values from decompressedArray.");

    for (int idx : Arrays.asList(0, decompressedValues.length / 2, decompressedValues.length - 2)) {
      long pageValueId = flattenedPage.getValues().get(idx);
      long value = flattenedPage.getColumnPageDict().decompressValue(pageValueId);

      Assert.assertEquals(value, decompressedValues[idx], "Expected correct value at index " + idx);
    }

    List<Long> multipleValueIds = flattenedPage.getValues()
        .getMultiple(Arrays.asList(0, decompressedValues.length / 2, decompressedValues.length - 2));
    Long[] multipleValues =
        flattenedPage.getColumnPageDict().decompressValues(multipleValueIds.toArray(new Long[multipleValueIds.size()]));

    Assert.assertEquals(
        Arrays.asList(multipleValues), Arrays.asList(decompressedValues[0],
            decompressedValues[decompressedValues.length / 2], decompressedValues[decompressedValues.length - 2]),
        "Expected correct results from getMultiple.");
  }

  @Test
  public void removeTwoThird() {
    // GIVEN
    int removedCount = 1 + (int) (decompressedValues.length * 2. / 3.);
    NavigableSet<Long> notAvailableRowIds =
        LongStream.range(0L, removedCount).mapToObj(Long::valueOf).collect(DiqubeCollectors.toNavigableSet());

    // WHEN
    ColumnPage flattenedPage = factory.createFlattenedColumnPageBuilder().withColName("1").withDelegate(delegate)
        .withFirstRowId(0L).withNotAvailableRowIds(notAvailableRowIds).build();

    // THEN
    // assert type of returned page to make sure we have a test for each type.
    Assert.assertTrue(flattenedPage instanceof FlattenedColumnPage, "Did expect a FlattenedColPage.");
    Assert.assertTrue(flattenedPage.getValues() instanceof IndexFilteringCompressedLongArray,
        "Did expect a IndexFilteringCompressedLongArray");

    Assert.assertNotNull(flattenedPage, "Expected to get a page returned.");

    long[] pageValueIds = flattenedPage.getValues().decompressedArray();
    Long[] pageValueIds2 = LongStream.of(pageValueIds).mapToObj(Long::valueOf).toArray(l -> new Long[l]);
    Long[] values = flattenedPage.getColumnPageDict().decompressValues(pageValueIds2);
    long[] expected = new long[decompressedValues.length - removedCount];
    System.arraycopy(decompressedValues, removedCount, expected, 0, expected.length);
    Assert.assertEquals(values, expected, "Expected correct values from decompressedArray.");

    for (int idx : Arrays.asList(0, 1)) {
      long pageValueId = flattenedPage.getValues().get(idx);
      long value = flattenedPage.getColumnPageDict().decompressValue(pageValueId);

      Assert.assertEquals(value, decompressedValues[removedCount + idx], "Expected correct value at index " + idx);
    }

    List<Long> multipleValueIds = flattenedPage.getValues().getMultiple(Arrays.asList(0, 1));
    Long[] multipleValues =
        flattenedPage.getColumnPageDict().decompressValues(multipleValueIds.toArray(new Long[multipleValueIds.size()]));

    Assert.assertEquals(Arrays.asList(multipleValues),
        Arrays.asList(decompressedValues[removedCount], decompressedValues[removedCount + 1]),
        "Expected correct results from getMultiple.");
  }

  @Test
  public void removeHalf() {
    // GIVEN
    int removedCount = decompressedValues.length / 2;
    NavigableSet<Long> notAvailableRowIds =
        LongStream.range(0L, removedCount).mapToObj(Long::valueOf).collect(DiqubeCollectors.toNavigableSet());

    // WHEN
    ColumnPage flattenedPage = factory.createFlattenedColumnPageBuilder().withColName("1").withDelegate(delegate)
        .withFirstRowId(0L).withNotAvailableRowIds(notAvailableRowIds).build();

    // THEN
    // assert type of returned page to make sure we have a test for each type.
    Assert.assertTrue(!(flattenedPage instanceof FlattenedColumnPage), "Did not expect a FlattenedColPage.");

    Assert.assertNotNull(flattenedPage, "Expected to get a page returned.");

    long[] pageValueIds = flattenedPage.getValues().decompressedArray();
    Long[] pageValueIds2 = LongStream.of(pageValueIds).mapToObj(Long::valueOf).toArray(l -> new Long[l]);
    Long[] values = flattenedPage.getColumnPageDict().decompressValues(pageValueIds2);
    long[] expected = new long[decompressedValues.length - removedCount];
    System.arraycopy(decompressedValues, removedCount, expected, 0, expected.length);
    Assert.assertEquals(values, expected, "Expected correct values from decompressedArray.");

    for (int idx : Arrays.asList(0, 1, 4)) {
      long pageValueId = flattenedPage.getValues().get(idx);
      long value = flattenedPage.getColumnPageDict().decompressValue(pageValueId);

      Assert.assertEquals(value, decompressedValues[removedCount + idx], "Expected correct value at index " + idx);
    }

    List<Long> multipleValueIds = flattenedPage.getValues().getMultiple(Arrays.asList(0, 1, expected.length - 1));
    Long[] multipleValues =
        flattenedPage.getColumnPageDict().decompressValues(multipleValueIds.toArray(new Long[multipleValueIds.size()]));

    Assert.assertEquals(
        Arrays.asList(multipleValues), Arrays.asList(decompressedValues[removedCount],
            decompressedValues[removedCount + 1], decompressedValues[removedCount + expected.length - 1]),
        "Expected correct results from getMultiple.");
  }
}
