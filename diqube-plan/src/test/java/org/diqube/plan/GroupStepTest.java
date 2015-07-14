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
package org.diqube.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.diqube.data.ColumnType;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupDeltaConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironmentFactory;
import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.execution.steps.GroupStep;
import org.diqube.execution.steps.ResolveColumnDictIdsStep;
import org.diqube.execution.steps.ResolveValuesStep;
import org.diqube.execution.steps.RowIdSinkStep;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link GroupStep}.
 *
 * @author Bastian Gloeckle
 */
public class GroupStepTest {

  private static final String COL_A = "colA";
  private static final String COL_B = "colB";
  private static final String COL_C = "colC";

  private AnnotationConfigApplicationContext dataContext;
  private ColumnShardBuilderManager columnShardBuilderManager;
  private TableFactory tableFactory;
  private ExecutionEnvironmentFactory executionEnvironmentFactory;
  private Map<String, StandardColumnShard> columns;

  private Map<String, Map<Long, Object>> resultValues;
  private Map<Long, List<Long>> resultFullGroups;
  private List<Map<Long, List<Long>>> resultDeltaGroups;

  @BeforeMethod
  public void setUp() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    ColumnShardBuilderFactory columnBuilderFactory = dataContext.getBean(ColumnShardBuilderFactory.class);
    LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG);
    columnShardBuilderManager = columnBuilderFactory.createColumnShardBuilderManager(colInfo, 0L);
    tableFactory = dataContext.getBean(TableFactory.class);
    executionEnvironmentFactory = dataContext.getBean(ExecutionEnvironmentFactory.class);

    resultValues = new HashMap<>();
    resultDeltaGroups = new ArrayList<>();
    resultFullGroups = new HashMap<>();

    columns = new HashMap<>();
  }

  @Test
  public void testOneColumn() throws Exception {
    // GIVEN
    // One-col grouping
    Long[] colAValues = new Long[] { 1L, 10L, Long.MIN_VALUE, -599L, 1L, 10L };
    columnShardBuilderManager.addValues(COL_A, colAValues, 0L);

    Long[] colBValues = new Long[] { -100L, -200L, Long.MAX_VALUE, -300L, 50L, 60L };
    columnShardBuilderManager.addValues(COL_B, colBValues, 0L);

    TableShard table = buildTable(columnShardBuilderManager);
    ExecutionEnvironment env = executionEnvironmentFactory.createExecutionEnvironment(table);

    // RowIdEquals and ResolveValue steps
    List<AbstractThreadedExecutablePlanStep> steps =
        createExecutableSteps(env, new String[] { COL_A }, new String[] { COL_A });

    // WHEN
    // executing the steps
    for (AbstractThreadedExecutablePlanStep step : steps) {
      step.run();
    }

    // THEN
    Assert.assertTrue(resultValues.containsKey(COL_A), "Result for col a expected");
    // group by as grouped the 1L and 10L values in COL_A
    Set<Object> expectedValues = new HashSet<>();
    expectedValues.add(1L);
    expectedValues.add(10L);
    expectedValues.add(Long.MIN_VALUE);
    expectedValues.add(-599L);
    Assert.assertEquals(new HashSet<Object>(resultValues.get(COL_A).values()), expectedValues,
        "Correct result for col a expected");

    Assert.assertEquals(resultFullGroups.size(), 4, "Expected 4 groups");
    Map<Long, Set<Long>> expectedValueToRowIds = new HashMap<>();
    // group with value 1L has rowIds 0 and 4
    expectedValueToRowIds.put(1L, new HashSet<Long>(Arrays.asList(new Long[] { 0L, 4L })));
    // group with value 10L has rowIds 1 and 5
    expectedValueToRowIds.put(10L, new HashSet<Long>(Arrays.asList(new Long[] { 1L, 5L })));
    // group with value Long.MIN_VALUE has rowId 2
    expectedValueToRowIds.put(Long.MIN_VALUE, new HashSet<Long>(Arrays.asList(new Long[] { 2L })));
    // group with value -599L has rowId 3
    expectedValueToRowIds.put(-599L, new HashSet<Long>(Arrays.asList(new Long[] { 3L })));

    Map<Long, Set<Long>> resultValueToRowIdMap = new HashMap<>();
    resultFullGroups.entrySet().stream().map(new Function<Entry<Long, List<Long>>, Pair<Long, Set<Long>>>() {
      @Override
      public Pair<Long, Set<Long>> apply(Entry<Long, List<Long>> t) {
        long groupId = t.getKey();

        long value = resolveValueForRowId(columns.get(COL_A), groupId);
        return new Pair<Long, Set<Long>>(value, new HashSet<Long>(t.getValue()));
      }
    }).forEach(pair -> resultValueToRowIdMap.put(pair.getLeft(), pair.getRight()));

    Assert.assertEquals(resultValueToRowIdMap, expectedValueToRowIds, "Expected correct grouping of row IDs");

    assertDeltaGroupsEqualsFullGroups();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testTwoColumns() throws Exception {
    // GIVEN
    // Two-col grouping
    Long[] colAValues = new Long[] { 1L, 10L, Long.MIN_VALUE, -599L, 1L, 1L };
    columnShardBuilderManager.addValues(COL_A, colAValues, 0L);

    Long[] colBValues = new Long[] { -100L, -200L, Long.MAX_VALUE, -300L, -100L, 60L };
    columnShardBuilderManager.addValues(COL_B, colBValues, 0L);

    Long[] colCValues = new Long[] { 0L, 0L, 0L, 0L, 0L, 0L };
    columnShardBuilderManager.addValues(COL_C, colCValues, 0L);

    TableShard table = buildTable(columnShardBuilderManager);
    ExecutionEnvironment env = executionEnvironmentFactory.createExecutionEnvironment(table);

    // RowIdEquals and ResolveValue steps
    List<AbstractThreadedExecutablePlanStep> steps =
        createExecutableSteps(env, new String[] { COL_A, COL_B }, new String[] { COL_A, COL_B });

    // WHEN
    // executing the steps
    for (AbstractThreadedExecutablePlanStep step : steps) {
      step.run();
    }

    // THEN
    Assert.assertTrue(resultValues.containsKey(COL_A), "Result for col a expected");
    // group by as grouped the 1L and 10L values in COL_A
    Set<Object> expectedValues = new HashSet<>();
    expectedValues.add(1L);
    expectedValues.add(10L);
    expectedValues.add(Long.MIN_VALUE);
    expectedValues.add(-599L);
    Assert.assertEquals(new HashSet<Object>(resultValues.get(COL_A).values()), expectedValues,
        "Expected corected (hashed) values for col a");

    Collection<Long> longValues = (Collection) resultValues.get(COL_A).values();

    long countOfValue1 = longValues.stream().mapToLong(Long::longValue).filter(l -> l == 1L).count();
    Assert.assertEquals(countOfValue1, 2,
        "Expected value 1L to be returned twice in col A because it is part of two groups");
    Assert.assertEquals(longValues.size(), expectedValues.size() + 1,
        "Expected only value 1L to be available twice in result values of col a");

    Assert.assertTrue(resultValues.containsKey(COL_B), "Result for col b expected");
    // group by as grouped the 1L and 10L values in COL_A
    expectedValues = new HashSet<>();
    expectedValues.add(-100L);
    expectedValues.add(-200L);
    expectedValues.add(Long.MAX_VALUE);
    expectedValues.add(-300L);
    expectedValues.add(60L);
    Assert.assertEquals(new HashSet<Object>(resultValues.get(COL_B).values()), expectedValues,
        "Expected corected values for col b");

    Assert.assertEquals(resultValues.get(COL_B).values().size(), expectedValues.size(),
        "Expected no value to be available twice in result values of col b");

    Assert.assertEquals(resultFullGroups.size(), 5, "Expected 5 groups");
    Map<Pair<Long, Long>, Set<Long>> expectedValueToRowIds = new HashMap<>();
    // group with value A:1L B:-100L has rowIds 0 and 4
    expectedValueToRowIds.put(new Pair<>(1L, -100L), new HashSet<Long>(Arrays.asList(new Long[] { 0L, 4L })));
    // group with value A:1L B:60L has rowId 5
    expectedValueToRowIds.put(new Pair<>(1L, 60L), new HashSet<Long>(Arrays.asList(new Long[] { 5L })));
    // group with value A:10L B:-200L has rowId 1
    expectedValueToRowIds.put(new Pair<>(10L, -200L), new HashSet<Long>(Arrays.asList(new Long[] { 1L })));
    // group with value A:MIN B:MAX has rowId 2
    expectedValueToRowIds.put(new Pair<>(Long.MIN_VALUE, Long.MAX_VALUE),
        new HashSet<Long>(Arrays.asList(new Long[] { 2L })));
    // group with value A:-599L B:-300L has rowId 3
    expectedValueToRowIds.put(new Pair<>(-599L, -300L), new HashSet<Long>(Arrays.asList(new Long[] { 3L })));

    Map<Pair<Long, Long>, Set<Long>> resultValueToRowIdMap = new HashMap<>();
    resultFullGroups.entrySet().stream()
        .map(new Function<Entry<Long, List<Long>>, Pair<Pair<Long, Long>, Set<Long>>>() {
          @Override
          public Pair<Pair<Long, Long>, Set<Long>> apply(Entry<Long, List<Long>> t) {
            long groupId = t.getKey();

            long valueA = resolveValueForRowId(columns.get(COL_A), groupId);
            long valueB = resolveValueForRowId(columns.get(COL_B), groupId);
            return new Pair<Pair<Long, Long>, Set<Long>>(new Pair<Long, Long>(valueA, valueB),
                new HashSet<Long>(t.getValue()));
          }
        }).forEach(pair -> resultValueToRowIdMap.put(pair.getLeft(), pair.getRight()));

    Assert.assertEquals(resultValueToRowIdMap, expectedValueToRowIds, "Expected correct grouping of row IDs");

    assertDeltaGroupsEqualsFullGroups();
  }

  private void assertDeltaGroupsEqualsFullGroups() {
    Map<Long, Set<Long>> deltaGroupsJoined = new HashMap<>();
    for (Map<Long, List<Long>> delta : resultDeltaGroups) {
      for (Entry<Long, List<Long>> deltaEntry : delta.entrySet()) {
        if (!deltaGroupsJoined.containsKey(deltaEntry.getKey()))
          deltaGroupsJoined.put(deltaEntry.getKey(), new HashSet<>());
        deltaGroupsJoined.get(deltaEntry.getKey()).addAll(deltaEntry.getValue());
      }
    }

    Map<Long, Set<Long>> fullGroupsWithSet = new HashMap<>();
    for (Entry<Long, List<Long>> fullGroupEntry : resultFullGroups.entrySet())
      fullGroupsWithSet.put(fullGroupEntry.getKey(), new HashSet<>(fullGroupEntry.getValue()));

    Assert.assertEquals(deltaGroupsJoined, fullGroupsWithSet,
        "Delta changes should end up being the same as fullGroup results when merged");
  }

  private long resolveValueForRowId(StandardColumnShard column, long rowId) {
    ColumnPage page = column.getPages().floorEntry(rowId).getValue();
    long columPageValueId = page.getValues().get((int) (rowId - page.getFirstRowId()));
    long columnValueId = page.getColumnPageDict().decompressValue(columPageValueId);
    long value = ((LongDictionary) column.getColumnShardDictionary()).decompressValue(columnValueId);
    return value;
  }

  private TableShard buildTable(ColumnShardBuilderManager columnShardBuilderManager) {

    for (String colName : columnShardBuilderManager.getAllColumnsWithValues()) {
      StandardColumnShard newColumn = columnShardBuilderManager.build(colName);
      columns.put(colName, newColumn);
    }

    return tableFactory.createTableShard(columns.values());
  }

  private List<AbstractThreadedExecutablePlanStep> createExecutableSteps(ExecutionEnvironment env, String[] resolveCols,
      String[] groupByCols) {

    List<AbstractThreadedExecutablePlanStep> res = new ArrayList<>();
    int stepId = 0;

    RowIdSinkStep rowIdSinkStep = new RowIdSinkStep(stepId++, env);
    res.add(rowIdSinkStep);

    GroupStep groupStep = new GroupStep(stepId++, env, Arrays.asList(groupByCols));
    groupStep.wireOneInputConsumerToOutputOf(RowIdConsumer.class, rowIdSinkStep);
    groupStep.addOutputConsumer(new AbstractThreadedGroupConsumer(null) {

      @Override
      protected void allSourcesAreDone() {
      }

      @Override
      protected void doConsumeGroups(Map<Long, List<Long>> fullGroups) {
        resultFullGroups = fullGroups;
      }
    });
    groupStep.addOutputConsumer(new AbstractThreadedGroupDeltaConsumer(null) {

      @Override
      protected void allSourcesAreDone() {
      }

      @Override
      protected void doConsumeGroupDeltas(Map<Long, List<Long>> lastChangedGroups) {
        resultDeltaGroups.add(new HashMap<Long, List<Long>>(lastChangedGroups));
      }
    });
    res.add(groupStep);

    ResolveValuesStep resolveValuesStep = new ResolveValuesStep(stepId++);

    for (String resolveCol : resolveCols) {
      ResolveColumnDictIdsStep resolveDictIdStep = new ResolveColumnDictIdsStep(stepId++, env, resolveCol);
      resolveDictIdStep.wireOneInputConsumerToOutputOf(RowIdConsumer.class, groupStep);
      resolveValuesStep.wireOneInputConsumerToOutputOf(ColumnDictIdConsumer.class, resolveDictIdStep);
      res.add(resolveDictIdStep);
    }

    resolveValuesStep.addOutputConsumer(new AbstractThreadedColumnValueConsumer(null) {

      @Override
      protected void allSourcesAreDone() {
      }

      @Override
      protected void doConsume(String colName, Map<Long, Object> values) {
        if (!resultValues.containsKey(colName))
          resultValues.put(colName, new HashMap<Long, Object>());

        resultValues.get(colName).putAll(values);
      }
    });
    res.add(resolveValuesStep);

    return res;
  }
}
