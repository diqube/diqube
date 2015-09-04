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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.diqube.context.Profiles;
import org.diqube.data.ColumnType;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.lng.dict.LongDictionary;
import org.diqube.execution.consumers.AbstractThreadedColumnDictIdConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironmentFactory;
import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.execution.steps.ResolveColumnDictIdsStep;
import org.diqube.execution.steps.RowIdEqualsStep;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link RowIdEqualsStep}.
 *
 * @author Bastian Gloeckle
 */
public class RowIdEqualsStepTest {

  private static final String COL_A = "colA";
  private static final String COL_B = "colB";

  private AnnotationConfigApplicationContext dataContext;
  private ColumnShardBuilderManager columnShardBuilderManager;
  private TableFactory tableFactory;
  private ExecutionEnvironmentFactory executionEnvironmentFactory;

  private Map<String, Map<Long, Object>> testResult;

  @BeforeMethod
  public void setUp() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    ColumnShardBuilderFactory columnBuilderFactory = dataContext.getBean(ColumnShardBuilderFactory.class);
    LoaderColumnInfo colInfo = new LoaderColumnInfo(ColumnType.LONG);
    columnShardBuilderManager = columnBuilderFactory.createColumnShardBuilderManager(colInfo, 0L);
    tableFactory = dataContext.getBean(TableFactory.class);
    executionEnvironmentFactory = dataContext.getBean(ExecutionEnvironmentFactory.class);

    testResult = new HashMap<>();

    QueryUuid.setCurrentQueryUuidAndExecutionUuid(UUID.randomUUID(), UUID.randomUUID());
  }

  @AfterMethod
  public void cleanup() {
    QueryUuid.clearCurrent();
  }

  @Test
  public void testOneColumn() throws Exception {
    // GIVEN
    // One-col table with some values
    Long[] colAValues = new Long[] { 1L, 10L, Long.MIN_VALUE, -599L };
    columnShardBuilderManager.addValues(COL_A, colAValues, 0L);

    TableShard table = buildTable(columnShardBuilderManager);
    ExecutionEnvironment env = executionEnvironmentFactory.createExecutionEnvironment(table);

    // RowIdEquals and ResolveValue steps
    List<AbstractThreadedExecutablePlanStep> steps =
        createIdEqualsAndResolveSteps(env, COL_A, new Long[] { Long.MIN_VALUE }, COL_A);

    // WHEN
    // executing the steps
    for (AbstractThreadedExecutablePlanStep step : steps) {
      step.run();
    }

    // THEN
    Assert.assertTrue(testResult.containsKey(COL_A), "Result for col a expected");
    Map<Long, Object> expected = new HashMap<>();
    expected.put(2L, Long.MIN_VALUE);
    Assert.assertEquals(testResult.get(COL_A), expected, "Correct result for col a expected");
  }

  @Test
  public void testTwoColumns() throws Exception {
    // GIVEN
    // Two-col table with some values
    Long[] colAValues = new Long[] { 1L, 10L, Long.MIN_VALUE, -599L };
    columnShardBuilderManager.addValues(COL_A, colAValues, 0L);

    Long[] colBValues = new Long[] { -100L, -200L, Long.MAX_VALUE, -300L };
    columnShardBuilderManager.addValues(COL_B, colBValues, 0L);

    TableShard table = buildTable(columnShardBuilderManager);
    ExecutionEnvironment env = executionEnvironmentFactory.createExecutionEnvironment(table);

    // RowIdEquals and ResolveValue steps, search in one column, select from the other.
    List<AbstractThreadedExecutablePlanStep> steps =
        createIdEqualsAndResolveSteps(env, COL_A, new Long[] { Long.MIN_VALUE }, COL_B);

    // WHEN
    // executing the steps
    for (AbstractThreadedExecutablePlanStep step : steps) {
      step.run();
    }

    // THEN
    Assert.assertTrue(testResult.containsKey(COL_B), "Result for col b expected");
    Map<Long, Object> expected = new HashMap<>();
    expected.put(2L, Long.MAX_VALUE);
    Assert.assertEquals(testResult.get(COL_B), expected, "Correct result for col b expected");
  }

  private TableShard buildTable(ColumnShardBuilderManager columnShardBuilderManager) {
    List<StandardColumnShard> columns = new ArrayList<>();

    for (String colName : columnShardBuilderManager.getAllColumnsWithValues())
      columns.add(columnShardBuilderManager.buildAndFree(colName));

    return tableFactory.createTableShard("table", columns);
  }

  private List<AbstractThreadedExecutablePlanStep> createIdEqualsAndResolveSteps(ExecutionEnvironment env,
      String equalsCol, Object[] equalsValues, String resolveCol) {
    RowIdEqualsStep rowIdEqualsStep = new RowIdEqualsStep(0,
        Mockito.mock(QueryRegistry.class, Mockito.RETURNS_DEEP_STUBS), env, equalsCol, new Long[] { Long.MIN_VALUE });
    ResolveColumnDictIdsStep resolveValueStep =
        new ResolveColumnDictIdsStep(1, Mockito.mock(QueryRegistry.class, Mockito.RETURNS_DEEP_STUBS), env, resolveCol);
    resolveValueStep.wireOneInputConsumerToOutputOf(RowIdConsumer.class, rowIdEqualsStep);
    resolveValueStep.addOutputConsumer(new AbstractThreadedColumnDictIdConsumer(null) {
      @Override
      protected void allSourcesAreDone() {
      }

      @Override
      protected void doConsume(ExecutionEnvironment env, String colName, Map<Long, Long> rowIdToColumnDictId) {
        if (!testResult.containsKey(colName))
          testResult.put(colName, new HashMap<Long, Object>());

        Map<Long, Long> values = new HashMap<>();
        // resolve column dictionary id to actual value
        rowIdToColumnDictId.forEach((rowId, colDictValueId) -> {
          LongDictionary<?> columnValueDict = env.getLongColumnShard(colName).getColumnShardDictionary();
          values.put(rowId, columnValueDict.decompressValue(colDictValueId));
        });

        testResult.get(colName).putAll(values);
      }
    });
    return Arrays.asList(new AbstractThreadedExecutablePlanStep[] { rowIdEqualsStep, resolveValueStep });
  }

}
