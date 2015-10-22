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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.cache.ColumnShardCache;
import org.diqube.execution.cache.ColumnShardCacheRegistry;
import org.diqube.execution.cache.DefaultColumnShardCache;
import org.diqube.execution.cache.DefaultColumnShardCacheTestUtil;
import org.diqube.execution.cache.WritableColumnShardCache;
import org.diqube.execution.steps.RepeatedProjectStep;
import org.diqube.loader.LoadException;
import org.diqube.server.execution.AbstractDiqlExecutionTest;
import org.diqube.util.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

/**
 * Tests the logic of {@link RepeatedProjectStep} to create only those output columns that are not yet available = that
 * are not available in the cache.
 * 
 * <p>
 * As this is a slightly special handling in that step, we have a separate test which tests all combinations of column
 * surviving a cache-clean of an example table/query situation.
 *
 * @author Bastian Gloeckle
 */
public class LongRepeatedProjectionCacheExecutionTest extends AbstractDiqlExecutionTest<Long> {
  private static final String TABLE_JSON = //
      "[ { \"a\": 1, \"b\": [ { \"c\": 20 }, { \"c\": 30 } ] },"//
          + "{ \"a\": 1, \"b\": [ { \"c\": 40 } ] } " //
          + " ]";

  private String projectedColName1;
  private String projectedColName2;
  private String projectedColNameLength;

  public LongRepeatedProjectionCacheExecutionTest() {
    super(ColumnType.LONG, new LongTestDataProvider());
  }

  @BeforeMethod
  @Override
  public void setUp() {
    super.setUp();

    projectedColName1 = repeatedColNameGen.repeatedAtIndex(functionBasedColumnNameBuilderFactory.create()
        .withFunctionName("add").addParameterColumnName("b[*].c").addParameterLiteralLong(1L).build(), 0);
    projectedColName2 = repeatedColNameGen.repeatedAtIndex(functionBasedColumnNameBuilderFactory.create()
        .withFunctionName("add").addParameterColumnName("b[*].c").addParameterLiteralLong(1L).build(), 1);
    projectedColNameLength = repeatedColNameGen.repeatedLength(functionBasedColumnNameBuilderFactory.create()
        .withFunctionName("add").addParameterColumnName("b[*].c").addParameterLiteralLong(1L).build());
  }

  @Test
  public void repeatedPartlyCacheInvalidation1() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColName1)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation2() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColName1, projectedColName2)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation3() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(
        new HashSet<>(Arrays.asList(projectedColName1, projectedColName2, projectedColNameLength)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation4() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColName2, projectedColNameLength)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation5() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColName1, projectedColNameLength)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation6() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColName2)));
  }

  @Test
  public void repeatedPartlyCacheInvalidation7() throws LoadException, InterruptedException, ExecutionException {
    genericRepeatedPartlyCacheInvalidation(new HashSet<>(Arrays.asList(projectedColNameLength)));
  }

  /**
   * Implementation of a test.
   * 
   * <p>
   * This executes the following:
   * 
   * <ul>
   * <li>Initialize table where we can execute repeated projects
   * <li>Execute a repeated project
   * <li>Validate correct result
   * <li>Clear {@link ColumnShardCache}, but leave specific columns in there
   * <li>Execute repeated project again
   * <li>Validate correct result again
   * </ul>
   * 
   * This validates that the {@link RepeatedProjectStep} handles situations correctly, where only a few of the repeated
   * output columns are available in the cache.
   * 
   * <p>
   * The interesting columns (= the output cols of the repeated project step) are:
   * 
   * <ul>
   * <li>{@link #projectedColName1}
   * <li>{@link #projectedColName2}
   * <li>{@link #projectedColNameLength}
   * </ul>
   * 
   * @param columnNamesOfColumnsToKeepInCache
   *          A combination of the "interesting columns" that should survive in the cache when cleaning it.
   */
  private void genericRepeatedPartlyCacheInvalidation(Set<String> columnNamesOfColumnsToKeepInCache)
      throws LoadException, InterruptedException, ExecutionException {
    initializeFromJson(TABLE_JSON);

    // make some data structures readily usable.
    ColumnShardCacheRegistry cacheRegistry = dataContext.getBean(ColumnShardCacheRegistry.class);
    WritableColumnShardCache cache = cacheRegistry.getOrCreateColumnShardCache(TABLE);

    Set<String> allInterestingColumnnames =
        new HashSet<>(Arrays.asList(projectedColName1, projectedColName2, projectedColNameLength));

    // as we execute the query multiple times, the following supplier will create and execute a new query and return the
    // Future on the plan.
    List<ExecutorService> executorsCreated = new ArrayList<>();
    Supplier<Future<?>> createAndExecutePlan = new Supplier<Future<?>>() {
      @Override
      public Future<?> get() {
        ExecutablePlan plan = buildExecutablePlan("select a, sum(sum(add(b[*].c, 1))) from " + TABLE + " group by a");
        ExecutorService executor = executors.newTestExecutor(plan.preferredExecutorServiceSize());
        executorsCreated.add(executor);
        return plan.executeAsynchronously(executor);
      }
    };

    // asserts a correct result after executing the query.
    Supplier<Void> assertCorrectResult = new Supplier<Void>() {
      @Override
      public Void get() {
        String resAggColName = functionBasedColumnNameBuilderFactory.create().withFunctionName("sum")
            .addParameterColumnName(functionBasedColumnNameBuilderFactory.create().withFunctionName("sum") //
                .addParameterColumnName( //
                    functionBasedColumnNameBuilderFactory.create().withFunctionName("add") //
                        .addParameterColumnName("b[*].c").addParameterLiteralLong(1L).build()
                        + repeatedColNameGen.allEntriesIdentifyingSubstr())
                .build())
            .build();

        Assert.assertTrue(resultValues.containsKey("a"), "Expected to have a result for col");
        Assert.assertTrue(resultValues.containsKey(resAggColName),
            "Expected that there's results for the aggregation func");
        Assert.assertEquals(resultValues.keySet().size(), 2, "Expected to have results for correct number of cols");

        Assert.assertEquals(resultValues.get("a").size(), 1, "Expected to receive a specific amout of rows");
        Assert.assertEquals(resultValues.get(resAggColName).size(), 1, "Expected to receive a specific amout of rows");

        Set<Pair<Long, Long>> expected = new HashSet<>();
        expected.add(new Pair<>(1L, 93L));

        Set<Pair<Long, Long>> actual = new HashSet<>();

        for (long rowId : resultValues.get("a").keySet())
          actual.add(new Pair<>(resultValues.get("a").get(rowId), resultValues.get(resAggColName).get(rowId)));

        Assert.assertEquals(actual, expected, "Expected correct result values");
        return null;
      }
    };

    // Ok, let's start testing!
    try {
      createAndExecutePlan.get().get(); // execute and wait until done

      assertCorrectResult.get();

      // assert that the result columns of the repeated project were put in the cache, otherwise the rest of the
      // test does not make sense.
      Assert.assertNotNull(cache.getCachedColumnShard(0L, projectedColName1), "Expected [0] to be inside cache.");
      Assert.assertNotNull(cache.getCachedColumnShard(0L, projectedColName2), "Expected [1] to be inside cache.");
      Assert.assertNotNull(cache.getCachedColumnShard(0L, projectedColNameLength),
          "Expected [length] to be inside cache.");

      // Now we take care of keeping only those columns in the cache that were requested to be kept in it!

      Set<String> columnShardNamesToRemoveFromCache =
          cache.getAllCachedColumnShards(0L).stream().map(c -> c.getName()).collect(Collectors.toSet());
      columnShardNamesToRemoveFromCache.removeAll(columnNamesOfColumnsToKeepInCache);

      Set<ColumnShard> cachedColumnShards = cache.getAllCachedColumnShards(0L).stream()
          .filter(colShard -> columnNamesOfColumnsToKeepInCache.contains(colShard.getName()))
          .collect(Collectors.toSet());

      // remove not-wanted cols from cache.
      for (String shardName : columnShardNamesToRemoveFromCache)
        DefaultColumnShardCacheTestUtil.removeFromCache((DefaultColumnShardCache) cache, 0L, shardName);

      // -> expected: the other column shards that was cached is now evicted from the cache.
      for (String colNameNotLongerInCache : Sets.difference(allInterestingColumnnames,
          columnNamesOfColumnsToKeepInCache))
        Assert.assertNull(cache.getCachedColumnShard(0L, colNameNotLongerInCache),
            "Expected " + colNameNotLongerInCache + " to NOT be inside cache.");
      for (String colNameInCache : columnNamesOfColumnsToKeepInCache)
        Assert.assertNotNull(cache.getCachedColumnShard(0L, colNameInCache),
            "Expected " + colNameInCache + " to be in cache.");

      // Ok, now only those columns that were requested are in the cache (and the mocked one, but we ignore that).
      createAndExecutePlan.get().get(); // execute and wait

      // assert result is good.
      assertCorrectResult.get();

      Set<ColumnShard> afterCachedColShards = columnNamesOfColumnsToKeepInCache.stream()
          .map(colName -> cache.getCachedColumnShard(0L, colName)).collect(Collectors.toSet());
      // the following assert basically compares using ==
      Assert.assertEquals(afterCachedColShards, cachedColumnShards,
          "Expected that cached col shards were not re-created.");
    } finally {
      executorsCreated.forEach(ex -> ex.shutdownNow());
    }
  }
}
