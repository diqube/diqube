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
package org.diqube.plan.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.diqube.data.ColumnType;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.TableRegistry;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedOrderedRowIdConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.loader.columnshard.ColumnShardBuilder;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.ColumnShardBuilderManager;
import org.diqube.plan.ExecutionPlanBuilder;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.util.Pair;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.BeforeMethod;

/**
 * Abstract base class for tests executing diql statements and inspecting the results.
 * 
 * <p>
 * All tests of subclasses are based on two columns ({@link #COL_A} and {@link #COL_B}) which have an arbitrary
 * {@link ColumnType} (from the point of view of this class). The actual values stored in the columns can be created
 * using a {@link TestDataProvider} (see {@link #dp}) which creates values of the correct data type. There is a set of
 * default values for the columns available in {@link #COL_A_DEFAULT_VALUES} and {@value #COL_B_DEFAULT_VALUES}.
 *
 * @param <T>
 *          Data type of the test columns.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractDiqlExecutionTest<T> {

  protected static final String TABLE = "TestTable";
  protected static final long VALUE_LENGTH = ColumnShardBuilder.PROPOSAL_ROWS + 100L;
  protected static final long VALUE_DELTA = 100000;
  protected static final String COL_A = "colA";
  protected Object[] COL_A_DEFAULT_VALUES;
  protected String[] COL_A_DEFAULT_VALUES_DIQL;
  protected static final String COL_B = "colB";
  protected Object[] COL_B_DEFAULT_VALUES;
  protected String[] COL_B_DEFAULT_VALUES_DIQL;

  protected AnnotationConfigApplicationContext dataContext;
  private ExecutionPlanBuilder executionPlanBuilder;
  /** Create a new {@link ColumnShardBuilderManager}, supply first row ID as parameter. */
  private Function<Long, ColumnShardBuilderManager> columnShardBuilderManagerSupplier;
  private TableFactory tableFactory;
  protected TableRegistry tableRegistry;

  protected boolean columnValueConsumerIsDone = false;
  /** Results of final {@link ColumnValueConsumer} of the pipeline, keyed by column name and row ID. */
  protected Map<String, Map<Long, T>> resultValues;
  /** Result of the final ordering is there was one. This contains ordered list of row IDs. */
  protected List<Long> resultOrderRowIds;

  /** {@link Object#notifyAll()} will be called on this object as soon as new data is available in {@link #resultValues} */
  protected Object newValuesNotify;
  /**
   * {@link Object#notifyAll()} will be called on this object as soon as new data is available in
   * {@link #resultOrderRowIds}
   */
  protected Object newOrderedRowIdsNotify;
  /**
   * Used to produce data of the data type that is used in out test columns.
   * 
   * <p>
   * The {@link TestDataProvider} will transform a simple long number into a value with the correct data type, whereas
   * all <, <=, >, >= relations will be just like expected of the corresponding long values.
   */
  protected TestDataProvider<T> dp;
  private ColumnType colType;

  /**
   * @param colType
   *          Type of the columns to be created.
   * @param dp
   *          A {@link TestDataProvider} that can create data whose type matches that of the columns.
   */
  public AbstractDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    this.colType = colType;
    this.dp = dp;
    COL_A_DEFAULT_VALUES = dp.emptyArray((int) VALUE_LENGTH);
    COL_A_DEFAULT_VALUES_DIQL = new String[(int) VALUE_LENGTH];
    for (int i = 0; i < VALUE_LENGTH; i++) {
      COL_A_DEFAULT_VALUES[i] = dp.v(i);
      COL_A_DEFAULT_VALUES_DIQL[i] = dp.vDiql(i);
    }
    COL_B_DEFAULT_VALUES = dp.emptyArray((int) VALUE_LENGTH);
    COL_B_DEFAULT_VALUES_DIQL = new String[(int) VALUE_LENGTH];
    for (int i = 0; i < VALUE_LENGTH; i++) {
      COL_B_DEFAULT_VALUES[i] = dp.v(VALUE_DELTA + i);
      COL_B_DEFAULT_VALUES_DIQL[i] = dp.vDiql(VALUE_DELTA + i);
    }
  }

  @BeforeMethod
  public void setUp() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.scan("org.diqube");
    dataContext.refresh();

    columnValueConsumerIsDone = false;
    resultValues = new ConcurrentHashMap<>();

    resultOrderRowIds = null;
    newValuesNotify = new Object();
    newOrderedRowIdsNotify = new Object();

    executionPlanBuilder =
        dataContext.getBean(ExecutionPlanBuilderFactory.class).createExecutionPlanBuilder()
            .withFinalColumnValueConsumer(new AbstractThreadedColumnValueConsumer(null) {

              @Override
              protected void allSourcesAreDone() {
                columnValueConsumerIsDone = true;
              }

              @Override
              protected synchronized void doConsume(String colName, Map<Long, Object> values) {
                if (!resultValues.containsKey(colName))
                  resultValues.put(colName, new HashMap<>());

                @SuppressWarnings({ "unchecked", "rawtypes" })
                Map<Long, T> valuesLongLong = (((Map) values));

                resultValues.get(colName).putAll(valuesLongLong);

                synchronized (newValuesNotify) {
                  newValuesNotify.notifyAll();
                }
              }
            }).withFinalOrderedRowIdConsumer(new AbstractThreadedOrderedRowIdConsumer(null) {

              @Override
              protected void allSourcesAreDone() {
              }

              @Override
              protected void doConsumeOrderedRowIds(List<Long> rowIds) {
                resultOrderRowIds = rowIds;
                synchronized (newOrderedRowIdsNotify) {
                  newOrderedRowIdsNotify.notifyAll();
                }
              }
            });

    ColumnShardBuilderFactory columnBuilderFactory = dataContext.getBean(ColumnShardBuilderFactory.class);
    LoaderColumnInfo colInfo = new LoaderColumnInfo(colType);
    columnShardBuilderManagerSupplier =
        (firstRowIdInShard) -> columnBuilderFactory.createColumnShardBuilderManager(colInfo, firstRowIdInShard);
    tableFactory = dataContext.getBean(TableFactory.class);
    tableRegistry = dataContext.getBean(TableRegistry.class);
  }

  /**
   * Build an {@link ExecutablePlan} for a query master from the given diql.
   */
  protected ExecutablePlan buildExecutablePlan(String diql) {
    ExecutablePlan masterExecutablePlan = executionPlanBuilder.fromDiql(diql).build();
    return masterExecutablePlan;
  }

  /**
   * Initialize a table with two columns in one table shard containing the given values.
   */
  protected void initializeSimpleTable(Object[] colAValues, Object[] colBValues) throws IllegalStateException {
    initializeMultiShardTable(Arrays.asList(new Pair[] { new Pair<Object[], Object[]>(colAValues, colBValues) }));
  }

  /**
   * Initialize a table with two columns, but consisting of multiple TableShards. For each TableShard to be created, the
   * provided list should contain one pair of values for colA and colB.
   */
  protected void initializeMultiShardTable(List<Pair<Object[], Object[]>> shardValues) throws IllegalStateException {
    List<TableShard> tableShards = new ArrayList<>();

    long firstRowId = 0;

    for (Pair<Object[], Object[]> shardColumnPair : shardValues) {
      ColumnShardBuilderManager columnShardBuilderManager = columnShardBuilderManagerSupplier.apply(firstRowId);

      Object[] colAValues = shardColumnPair.getLeft();
      Object[] colBValues = shardColumnPair.getRight();
      columnShardBuilderManager.addValues(COL_A, colAValues, firstRowId);
      columnShardBuilderManager.addValues(COL_B, colBValues, firstRowId);
      List<StandardColumnShard> columns = new ArrayList<>();
      for (String colName : columnShardBuilderManager.getAllColumnsWithValues())
        columns.add(columnShardBuilderManager.build(colName));
      TableShard tableShard = tableFactory.createTableShard(columns);
      tableShards.add(tableShard);

      firstRowId += colAValues.length;
    }

    tableRegistry.addTable(TABLE, tableFactory.createTable(TABLE, tableShards));
  }

}
