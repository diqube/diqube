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
package org.diqube.server.execution.lng;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.diqube.data.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.cache.ColumnShardCacheRegistry;
import org.diqube.execution.cache.DefaultColumnShardCache;
import org.diqube.execution.cache.DefaultColumnShardCacheTestUtil;
import org.diqube.loader.LoadException;
import org.diqube.plan.exception.ValidationException;
import org.diqube.server.execution.AbstractCacheDoubleDiqlExecutionTest;
import org.diqube.server.execution.CacheDoubleTestUtil.IgnoreInCacheDoubleTestUtil;
import org.diqube.util.DoubleUtil;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

/**
 * Test the column aggregation and repeated projection features on LONG columns.
 * 
 * The column aggregation aggregates the values of multiple columns of the same row to one new value. This is used if an
 * aggregation is executed on repeated fields for example. In contrast to that, aggregation that is executed on a GROUP
 * BY does aggregate values of the same column but multiple rows.
 *
 * @author Bastian Gloeckle
 */
public class LongColumnAggregationAndRepeatedProjectionDiqlExecutionTest
    extends AbstractCacheDoubleDiqlExecutionTest<Long> {
  public LongColumnAggregationAndRepeatedProjectionDiqlExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @Test
  public void simpleColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson("[ { \"a\": 1, \"b\": [ { \"c\": 0 }, { \"c\": 10 } ] } ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName("b[*].c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 1, "Expected to receive a specific amout of rows");

      Assert.assertEquals((long) resultValues.get("a").values().iterator().next(), 1L, "Expected correct result value");
      Assert.assertEquals((long) resultValues.get(resAggColName).values().iterator().next(), 5L,
          "Expected correct result value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": 0 }, { \"c\": 10 } ] }," + //
            "{ \"a\": 2, \"b\": [ { \"c\": 0 }, { \"c\": 100 } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName("b[*].c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 5L));
      expected.add(new Pair<>(2L, 50L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowTwoLevelColAggregation() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ { \"d\": 20 }, { \"d\": 30 } ] }, { \"c\": [ { \"d\": 100 }, { \"d\": 25 }, { \"d\": 75 } ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ { \"d\": 400 }, { \"d\": 50 }, { \"d\": 150 } ] }, { \"c\": [ { \"d\": 1050 }, { \"d\": 850 } ] } ] }"
            + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c[*].d)) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName("b[*].c[*].d").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 50L));
      expected.add(new Pair<>(2L, 500L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowTwoLevelColAggregationWithoutObjectsInArray()
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(b[*].c[*])) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName("b[*].c[*]").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 50L));
      expected.add(new Pair<>(2L, 500L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowTwoLevelColAggregationAfterProjection()
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"e\" : 10, \"c\": [ { \"d\": 20 }, { \"d\": 30 } ] }, " + //
            "{ \"e\": 5, \"c\": [ { \"d\": 100 }, { \"d\": 25 }, { \"d\": 75 } ] } ] },"//
    // 2nd top level:
            + "{ \"a\": 2, " + //
            "\"b\": [ { \"e\" : 1, \"c\": [ { \"d\": 400 }, { \"d\": 50 }, { \"d\": 150 } ] }, " + //
            "{ \"e\" : 1, \"c\": [ { \"d\": 1050 }, { \"d\": 850 } ] } ] }" //
            + //
            " ]");

    // expected result:
    // 1st top level: avg((20 + 10), (30 + 10), (100 + 5), (25 + 5), (75 + 5)) == 285 / 5 == 57
    // 2nd top level: avg((400 + 1), (50 + 1), (150 + 1), (1050 + 1), (850 + 1)) == 2505 / 5 == 501

    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(add(b[*].c[*].d, b[*].e))) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
                  .addParameterColumnName("b[*].c[*].d").addParameterColumnName("b[*].e").build()
                  + repeatedColNameGen.allEntriesManifestedSubstr())
              .build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 57L));
      expected.add(new Pair<>(2L, 501L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void twoRowTwoLevelColAggregationAfterProjection2()
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": [ { \"d\": 20 }, { \"d\": 30 } ] }, " + //
            "{  \"c\": [ { \"d\": 100 }, { \"d\": 25 }, { \"d\": 75 } ] } ] },"//
    // 2nd top level:
            + "{ \"a\": 2, " + //
            "\"b\": [ { \"c\": [ { \"d\": 400 }, { \"d\": 50 }, { \"d\": 150 } ] }, " + //
            "{  \"c\": [ { \"d\": 1050 }, { \"d\": 850 } ] } ] }" //
            + //
            " ]");

    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(add(b[*].c[*].d, 1))) from " + TABLE);
    // expected result:
    // 1st top level: avg((20 + 1), (30 + 1), (100 + 1), (25 + 1), (75 + 1)) == 255 / 5 == 51
    // 2nd top level: avg((400 + 1), (50 + 1), (150 + 1), (1050 + 1), (850 + 1)) == 2505 / 5 == 501
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
                  .addParameterColumnName("b[*].c[*].d").addParameterLiteralLong(1).build()
                  + repeatedColNameGen.allEntriesManifestedSubstr())
              .build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 51L));
      expected.add(new Pair<>(2L, 501L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void selectProjected() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a, add(b[*].c[*].d, 1) from " + TABLE);
  }

  @Test(expectedExceptions = ValidationException.class)
  public void whereProjected() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a from " + TABLE + " where add(b[*].c[*].d, 1) = 5");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void groupProjected() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a from " + TABLE + " group by add(b[*].c[*].d, 1)");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void havingProjected() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a from " + TABLE + " group by a having add(b[*].c[*].d, 1) = 5");
  }

  @Test(expectedExceptions = ValidationException.class)
  public void orderProjected() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a from " + TABLE + " order by add(b[*].c[*].d, 1)");
  }

  @Test
  public void orderProjectedAggregated() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    ExecutablePlan plan = buildExecutablePlan("select a from " + TABLE + " order by avg(add(b[*].c, 1))");
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertEquals(resultValues.keySet().size(), 1, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");

      Assert.assertEquals((long) Iterables.getOnlyElement(resultValues.get("a").values()), 1L,
          "Expected correct result value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void whereProjectedAggregated() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    ExecutablePlan plan = buildExecutablePlan("select a from " + TABLE + " where avg(add(b[*].c, 1)) = 26.");
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertEquals(resultValues.keySet().size(), 1, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");

      Assert.assertEquals((long) Iterables.getOnlyElement(resultValues.get("a").values()), 1L,
          "Expected correct result value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void groupProjectedAggregated() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    ExecutablePlan plan = buildExecutablePlan("select a from " + TABLE + " group by avg(add(b[*].c, 1))");
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertEquals(resultValues.keySet().size(), 1, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");

      Assert.assertEquals((long) Iterables.getOnlyElement(resultValues.get("a").values()), 1L,
          "Expected correct result value");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void selectProjectedAggregated() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(add(b[*].c, 1))) from " + TABLE);
    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round")
          .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("avg")
              .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
                  .addParameterColumnName("b[*].c").addParameterLiteralLong(1).build()
                  + repeatedColNameGen.allEntriesManifestedSubstr())
              .build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 1, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 26L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(expectedExceptions = ValidationException.class)
  public void havingProjectedAggregated() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, " + //
            "\"b\": [ { \"c\": 20 }, { \"c\": 30 } ] } ]");

    buildExecutablePlan("select a from " + TABLE + " group by a having avg(add(b[*].c[*].d, 1)) = 5");
  }

  @Test
  public void rowAndColAggregationAndProjection() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }," //
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 10, 0, 3 ] }, { \"c\": [ 7, 20 ] } ] }" + //
            " ]");
    ExecutablePlan plan =
        buildExecutablePlan("select a, round(avg(avg(add(b[*].c[*], 1)))) from " + TABLE + " group by a");

    // Explanation: from inner functions to outer:
    // add(b[*].c[*], 1) is REPEATED_PROJECT
    // avg(..) is AGGREGATION_COL (outputs one value per row)
    // avg(..) is AGGREGATION_ROW (aggregates the two rows with a==2)
    // round(..) is a PROJECT

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round") //
          .addParameterColumnName(//
              functionBasedColumnNameBuilderFactory.create().withFunctionName("avg") //
                  .addParameterColumnName( //
                      functionBasedColumnNameBuilderFactory.create().withFunctionName("avg") //
                          .addParameterColumnName( //
                              functionBasedColumnNameBuilderFactory.create().withFunctionName("add") //
                                  .addParameterColumnName("b[*].c[*]") //
                                  .addParameterLiteralLong(1).build() + repeatedColNameGen.allEntriesManifestedSubstr())
                          .build())
                  .build()) //
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 2, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 51L));
      expected.add(new Pair<>(2L, (501L + 9L) / 2));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void rowAndColAggregationAndProjectionWithHaving()
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }," //
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 10, 0, 3 ] }, { \"c\": [ 7, 20 ] } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, round(avg(avg(add(b[*].c[*], 1)))) from " + TABLE
        + " group by a having avg(avg(add(b[*].c[*], 1))) > 100.");

    // Explanation: from inner functions to outer:
    // add(b[*].c[*], 1) is REPEATED_PROJECT
    // avg(..) is AGGREGATION_COL (outputs one value per row)
    // avg(..) is AGGREGATION_ROW (aggregates the two rows with a==2)
    // round(..) is a PROJECT

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("round") //
          .addParameterColumnName(//
              functionBasedColumnNameBuilderFactory.create().withFunctionName("avg") //
                  .addParameterColumnName( //
                      functionBasedColumnNameBuilderFactory.create().withFunctionName("avg") //
                          .addParameterColumnName( //
                              functionBasedColumnNameBuilderFactory.create().withFunctionName("add") //
                                  .addParameterColumnName("b[*].c[*]") //
                                  .addParameterLiteralLong(1).build() + repeatedColNameGen.allEntriesManifestedSubstr())
                          .build())
                  .build()) //
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultHavingRowIds.length, 1,
          "Expected to have specific number of results that passed the HAVING.");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(2L, (501L + 9L) / 2));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultHavingRowIds)
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void colAggregationWithFunctionParam() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }," //
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 10, 0, 3 ] }, { \"c\": [ 7, 20 ] } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a from " + TABLE + " where any(7, b[*].c[*]) = 1 group by a");

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to have specific number of results.");

      Assert.assertEquals((long) resultValues.get("a").values().iterator().next(), 2L,
          "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void colAggregationWithFunctionParam2() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }," //
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 10, 0, 3 ] }, { \"c\": [ 7, 20 ] } ] }" + //
            " ]");
    ExecutablePlan plan = buildExecutablePlan("select a from " + TABLE + " where any(75, b[*].c[*]) = 1");

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to have specific number of results.");

      Assert.assertEquals((long) resultValues.get("a").values().iterator().next(), 1L,
          "Expected correct result values");

    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void sumTest() throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": [ 20, 30 ] }, { \"c\": [ 100, 25, 75  ] } ] },"
            + "{ \"a\": 2, \"b\": [ { \"c\": [ 400, 50, 150 ] }, { \"c\": [ 1050, 850 ] } ] }" //
            + " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, sum(b[*].c[*]) from " + TABLE);

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sum") //
          .addParameterColumnName("b[*].c[*]").build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected correct number of res rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 20L + 30L + 100L + 25L + 75L));
      expected.add(new Pair<>(2L, 400L + 50L + 150L + 1050L + 850L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");

    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void repeatedCheckDefaultValueFallback() throws LoadException, InterruptedException, ExecutionException {
    // second row does not have any value for b[1].c, but ColumnAggregationStep nevertheless tries to resolve the value
    // -> it needs to be a default value. The step will later then inspect the actual length of the array of a row.
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": [ { \"c\": 20 }, { \"c\": 30 } ] },"//
            + "{ \"a\": 1, \"b\": [ { \"c\": 40 } ] } " //
            + " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, sum(sum(b[*].c)) from " + TABLE + " group by a");

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sum") //
          .addParameterColumnName( //
              functionBasedColumnNameBuilderFactory.create().withFunctionName("sum") //
                  .addParameterColumnName("b[*].c").build())
          .build();

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(resAggColName),
          "Expected that there's results for the aggregation func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(resAggColName).size(), 1, "Expected to receive a specific amout of rows");

      Set<Pair<Long, Long>> expected = new HashSet<>();
      expected.add(new Pair<>(1L, 90L));

      Set<Pair<Long, Long>> actual = new HashSet<>();

      for (long rowId : resultValues.get("a").keySet())
        actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

      Assert.assertEquals(actual, expected, "Expected correct result values");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @IgnoreInCacheDoubleTestUtil
  public void colAggregationCachedProjectionNotExecuted()
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson( //
        "[ { \"a\": 1, \"b\": 5 },"//
            + "{ \"a\": 2, \"b\": 6 } " //
            + " ]");
    ExecutablePlan plan = buildExecutablePlan("select a, sub(add(b, 1), 1) from " + TABLE);

    ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();
    } finally {
      executor.shutdownNow();
    }

    ColumnShardCacheRegistry cacheReg = dataContext.getBean(ColumnShardCacheRegistry.class);
    DefaultColumnShardCache cache = (DefaultColumnShardCache) cacheReg.getColumnShardCache(TABLE);
    Collection<String> cachedShards =
        cache.getAllCachedColumnShards(0L).stream().map(shard -> shard.getName()).collect(Collectors.toList());

    String innerColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("add")
        .addParameterColumnName("b").addParameterLiteralLong(1).build();
    String outerColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sub")
        .addParameterColumnName(innerColName).addParameterLiteralLong(1).build();

    Assert.assertEquals(new HashSet<>(cachedShards), new HashSet<>(Arrays.asList(innerColName, outerColName)),
        "Expected to have correct columns in cache after executing query for the first time");

    // remove the inner column from the cache
    DefaultColumnShardCacheTestUtil.removeFromCache(cache, 0L, innerColName);

    // now execute a second time
    executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
    try {
      Future<?> future = plan.executeAsynchronously(executor);
      future.get();

      Assert.assertTrue(columnValueConsumerIsDone, "Source should have reported 'done'");
      Assert.assertTrue(future.isDone(), "Future should report done");
      Assert.assertFalse(future.isCancelled(), "Future should not report cancelled");

      Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
      Assert.assertTrue(resultValues.containsKey(outerColName), "Expected that there's results for the output func");
      Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

      Assert.assertEquals(resultValues.get("a").size(), 2, "Expected to receive a specific amout of rows");
      Assert.assertEquals(resultValues.get(outerColName).size(), 2, "Expected to receive a specific amout of rows");

      Map<Long, Long> expected = new HashMap<>();
      expected.put(1L, 5L);
      expected.put(2L, 6L);

      for (long rowId : resultValues.get("a").keySet()) {
        Long valueColA = resultValues.get("a").get(rowId);
        Long valueFn = resultValues.get(outerColName).get(rowId);

        Assert.assertTrue(DoubleUtil.equals(valueFn, expected.get(valueColA)),
            "Expected correct result for colA value '" + valueColA + "'. Expected: " + expected.get(valueColA)
                + " but was: " + valueFn);
      }

      // we expect to NOT have something in the cache for the innerColumn, as we (1) removed it and (2) the second
      // execution should not have executed that step.
      Assert.assertNull(cache.getCachedColumnShard(0L, innerColName),
          "Expected to have NOT have executed the calculation of the unneeded column");
    } finally {
      executor.shutdownNow();
    }

  }

}
