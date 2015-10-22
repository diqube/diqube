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
package org.diqube.execution.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.data.column.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.ColumnVersionManager;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.loader.columnshard.ColumnShardBuilderFactory;
import org.diqube.loader.columnshard.SparseColumnShardBuilder;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Builds a temporary sparse column out of values provided by a {@link ColumnValueConsumer}.
 * 
 * <p>
 * It builds a final column shard as soon as the input {@link ColumnValueConsumer} is fully done - if one is interested
 * in more updates, the {@link ColumnVersionBuiltConsumer} should be wired. The latter will receive as much updates as
 * possible with intermediate {@link ExecutionEnvironment}s containing the built column.
 * 
 * <p>
 * Input: 1 {@link ColumnValueConsumer}<br>
 * Output: {@link ColumnBuiltConsumer} and {@link ColumnVersionBuiltConsumer}
 *
 * @author Bastian Gloeckle
 */
public class BuildColumnFromValuesStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(BuildColumnFromValuesStep.class);

  private String colName;

  private AtomicBoolean sourceIsDone = new AtomicBoolean(false);

  private Object columnSync = new Object();
  /** All values of the column we're interested in, keyed by rowId. Sync access with {@link #columnSync}. */
  private Map<Long, Object> columnValues = new HashMap<>();
  /**
   * Those rowIds that have been updated since the last run of {@link #execute()}. Sync access with {@link #columnSync}.
   */
  private Set<Long> updatedRowIds = new HashSet<Long>();
  /** <code>true</code> if there was at least one update for our col since the last run of {@link #execute()} */
  private AtomicBoolean atLeastOneInterestingUpdate = new AtomicBoolean(false);

  private AbstractThreadedColumnValueConsumer columnValueConsumer = new AbstractThreadedColumnValueConsumer(this) {
    @Override
    protected void allSourcesAreDone() {
      BuildColumnFromValuesStep.this.sourceIsDone.set(true);
    }

    @Override
    protected void doConsume(String colName, Map<Long, Object> values) {
      if (!colName.equals(BuildColumnFromValuesStep.this.colName))
        return;

      synchronized (columnSync) {
        columnValues.putAll(values);
        updatedRowIds.addAll(values.keySet());
        atLeastOneInterestingUpdate.set(true);
      }
    }
  };

  private ColumnShardBuilderFactory columnShardBuilderFactory;

  private ExecutionEnvironment defaultEnv;

  private ColumnVersionManager columnVersionManager;

  public BuildColumnFromValuesStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      String colName, ColumnShardBuilderFactory columnShardBuilderFactory, ColumnVersionManager columnVersionManager) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.colName = colName;
    this.columnShardBuilderFactory = columnShardBuilderFactory;
    this.columnVersionManager = columnVersionManager;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof ColumnBuiltConsumer)
        && !(consumer instanceof ColumnVersionBuiltConsumer))
      throw new IllegalArgumentException("Only ColumnBuiltConsumer and ColumnVersionBuiltConsumer supported.");
  }

  @Override
  protected void execute() {
    // this is the last run of this execute method if the input source is fully done.
    boolean intermediateRun = !sourceIsDone.get();

    if (intermediateRun && !existsOutputConsumerOfType(ColumnVersionBuiltConsumer.class))
      // if this is NOT the last run (= there are more values to be provided), but there is no-one who'd listen to
      // intermediary updates, do not calculate them.
      return;

    if (intermediateRun && !atLeastOneInterestingUpdate.get())
      return;

    Map<Long, Object> values;
    Set<Long> curUpdatedRowIds;
    synchronized (columnSync) {
      atLeastOneInterestingUpdate.set(false);

      if (columnValues == null || columnValues.isEmpty()) {
        if (!intermediateRun) {
          // source is done but we did not receive any data. Do not build column, just report "done".
          forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
          doneProcessing();
          return;
        }
        return;
      }

      values = new HashMap<Long, Object>(columnValues);

      curUpdatedRowIds = updatedRowIds;
      updatedRowIds = new HashSet<>();
    }
    long numberOfRows = values.keySet().stream().max(Long::compare).get() + 1;

    SparseColumnShardBuilder<Object> columnShardBuilder =
        columnShardBuilderFactory.createSparseColumnShardBuilder(colName);

    columnShardBuilder.withValues(values);
    columnShardBuilder.withNumberOfRows(numberOfRows);
    ColumnShard newColumn = columnShardBuilder.build();

    // inform ColumnVersionBuiltConsumers
    if (existsOutputConsumerOfType(ColumnVersionBuiltConsumer.class)) {
      logger.trace("Building new column version for {} after adjusting rows (limt) {}", colName,
          Iterables.limit(curUpdatedRowIds, 500));
      VersionedExecutionEnvironment newEnv = columnVersionManager.createNewVersion(newColumn);
      forEachOutputConsumerOfType(ColumnVersionBuiltConsumer.class,
          c -> c.columnVersionBuilt(newEnv, colName, curUpdatedRowIds));
    }

    // if done, inform other consumers.
    if (!intermediateRun) {
      switch (newColumn.getColumnType()) {
      case STRING:
        defaultEnv.storeTemporaryStringColumnShard((StringColumnShard) newColumn);
        break;
      case LONG:
        defaultEnv.storeTemporaryLongColumnShard((LongColumnShard) newColumn);
        break;
      case DOUBLE:
        defaultEnv.storeTemporaryDoubleColumnShard((DoubleColumnShard) newColumn);
        break;
      }

      logger.trace("Built column {} from values received from a ColumnValueConsumer.", colName);
      forEachOutputConsumerOfType(ColumnBuiltConsumer.class, c -> c.columnBuilt(colName));
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>(Arrays.asList(new GenericConsumer[] { columnValueConsumer }));
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "colName=" + colName;
  }

}
