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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.diqube.execution.ColumnVersionBuiltHelper;
import org.diqube.execution.consumers.AbstractThreadedColumnBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.AbstractThreadedRowIdConsumer;
import org.diqube.execution.consumers.ColumnBuiltConsumer;
import org.diqube.execution.consumers.ColumnDictIdConsumer;
import org.diqube.execution.consumers.ColumnVersionBuiltConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Resolves Column shard Dictionary IDs for the rowIds in a specific column.
 * 
 * <p>
 * This step can optionally be executed on a column that still needs to be constructed. In that case, a
 * {@link ColumnBuiltConsumer} input needs to be specified which keeps this step up to date with the construction of
 * that column. In that case, an additional {@link ColumnVersionBuiltConsumer} could be specified. If no
 * {@link ColumnBuiltConsumer} is specified, then it is expected that the column is already available through the
 * default {@link ExecutionEnvironment}.
 * 
 * <p>
 * Input: 1 {@link RowIdConsumer} and 1 optional {@link ColumnBuiltConsumer}, 1 optional
 * {@link ColumnVersionBuiltConsumer} <br>
 * Output: {@link ColumnDictIdConsumer}s.
 * 
 * @author Bastian Gloeckle
 */
public class ResolveColumnDictIdsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(ResolveColumnDictIdsStep.class);

  private AtomicBoolean rowIdSourceIsEmpty = new AtomicBoolean(false);

  private ConcurrentLinkedDeque<Long> rowIds = new ConcurrentLinkedDeque<>();

  private RowIdConsumer rowIdConsumer = new AbstractThreadedRowIdConsumer(this) {
    @Override
    public void allSourcesAreDone() {
      ResolveColumnDictIdsStep.this.rowIdSourceIsEmpty.set(true);
    }

    @Override
    protected void doConsume(Long[] rowIds) {
      for (long rowId : rowIds)
        ResolveColumnDictIdsStep.this.rowIds.add(rowId);
    }
  };

  /** Only important if {@link #colBuiltConsumer} is wired */
  private AtomicBoolean sourceColumnIsBuilt = new AtomicBoolean(false);

  private AtomicBoolean colBuiltConsumerIsDone = new AtomicBoolean(false);

  private AbstractThreadedColumnBuiltConsumer colBuiltConsumer = new AbstractThreadedColumnBuiltConsumer(this) {
    @Override
    protected void doColumnBuilt(String colName) {
      if (colName.equals(ResolveColumnDictIdsStep.this.colName))
        ResolveColumnDictIdsStep.this.sourceColumnIsBuilt.set(true);
    }

    @Override
    protected void allSourcesAreDone() {
      colBuiltConsumerIsDone.set(true);
    }
  };

  private Object newestSync = new Object();
  /**
   * The {@link VersionedExecutionEnvironment} with the highest ID that has been provided up until now. Use this
   * {@link ExecutionEnvironment} for resolving any valus of columns when based on intermediary values. Sync access with
   * {@link #newestSync}.
   */
  private VersionedExecutionEnvironment newestTemporaryEnv = null;
  /**
   * Those row IDs that have been reported since the last run of {@link #execute()} as having their values changed..
   * Sync access with {@link #newestSync}.
   */
  private NavigableSet<Long> newestAdjustedRowIds = new ConcurrentSkipListSet<>();

  private AbstractThreadedColumnVersionBuiltConsumer columnVersionBuiltConsumer =
      new AbstractThreadedColumnVersionBuiltConsumer(this) {
        @Override
        protected void allSourcesAreDone() {
        }

        @Override
        protected void doColumnBuilt(VersionedExecutionEnvironment env, String colName, Set<Long> adjustedRowIds) {
          // TODO #8 act only if colName.equals(this.colName).
          synchronized (newestSync) {
            if (newestTemporaryEnv == null)
              newestTemporaryEnv = env;
            else if (newestTemporaryEnv.getVersion() < env.getVersion())
              newestTemporaryEnv = env;
            newestAdjustedRowIds.addAll(adjustedRowIds);
          }
        }
      };

  /** name of the col to resolve values of. */
  private String colName;

  private ExecutionEnvironment defaultEnv;

  /**
   * Row IDs that have been reported by {@link RowIdConsumer} for resolving. But up until now, there were no values
   * available for these rowIds, so we remember them to be resolved later. This can happen if
   * {@link ColumnVersionBuiltConsumer} is wired and we base our execution on intermediary values.
   */
  private NavigableSet<Long> notYetProcessedRowIds = new TreeSet<>();
  /**
   * All rowIds that we already resolved values of. We need to remember those in case any of these rowIds changes its
   * values (as reported by input {@link ColumnVersionBuiltConsumer}s) and we need to resolve it again.
   */
  private Set<Long> processedRowIds = new HashSet<>();

  public ResolveColumnDictIdsStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment defaultEnv,
      String colName) {
    super(stepId, queryRegistry);
    this.defaultEnv = defaultEnv;
    this.colName = colName;
  }

  @Override
  public void execute() {
    boolean intermediateRun = !(colBuiltConsumer.getNumberOfTimesWired() == 0 || sourceColumnIsBuilt.get());

    if (colBuiltConsumer.getNumberOfTimesWired() > 0 && colBuiltConsumerIsDone.get() && !sourceColumnIsBuilt.get()) {
      logger.debug("Waited for column {} to  be built, but it won't be built. Skipping.", colName);
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
      return;
    }

    NavigableSet<Long> curAdjustedRowIds;
    synchronized (newestSync) {
      // Fetch rowIds whose values have been adjusted. Note that this is not 100% thread-safe in case intermediateRun ==
      // true. Because in that case we will resolve the corresponding ExecutionEnvironment that should be used later
      // with another sync block - in between a new env might have arrived with new adjustedRowIds - as the set of
      // rowIds being reported only increases though, it is no problem to only execute on a set of adjustedRows on a
      // newer env, as we will resolve those other reported rowIds just one execution later.
      curAdjustedRowIds = newestAdjustedRowIds;
      newestAdjustedRowIds = new TreeSet<>();
    }

    ExecutionEnvironment env;
    if (!intermediateRun)
      env = defaultEnv;
    else {
      synchronized (newestSync) {
        env = newestTemporaryEnv;
        if (env == null || env.getColumnShard(colName) == null) {
          // re-remember those IDs we removed from the set already.
          newestAdjustedRowIds.addAll(curAdjustedRowIds);
          return;
        }
      }
    }

    // fetch row IDs whose columndictid should be resolved.
    NavigableSet<Long> activeRowIds = new TreeSet<>();
    Long rowId;
    while ((rowId = rowIds.poll()) != null)
      activeRowIds.add(rowId);

    if (intermediateRun) {
      // restrict active row IDs to only contain available rows and include & publish notYetProcessedRowIds.
      long maxAvailableRowId = new ColumnVersionBuiltHelper().publishActiveRowIds(env, Arrays.asList(colName),
          activeRowIds, notYetProcessedRowIds);

      if (maxAvailableRowId == -1L) {
        // our column is not built. Should not happen, but just to be sure...
        logger.warn("ColumnVersionBuiltHelper told us that our column is notr built. This should not happen.");
        return;
      }

      // adjust set of rows that have been adjusted - shrink them to the row IDs that are available. If other rowIds
      // have changed their value this is not interesting to us, because we did notyet resolve their values anyway.
      curAdjustedRowIds = curAdjustedRowIds.headSet(maxAvailableRowId, true);
    } else {
      activeRowIds.addAll(notYetProcessedRowIds);
      notYetProcessedRowIds.clear();
    }

    // be sure to resolve those row IDs fresh that we resolved already but whose value changed.
    activeRowIds.addAll(Sets.intersection(curAdjustedRowIds, processedRowIds));

    if (activeRowIds.size() > 0) {
      logger.trace("Resolving column dict IDs of col {} based on ExecutionEnv {} at row IDs (limit, {}) {}", colName,
          env, activeRowIds.size(), Iterables.limit(activeRowIds, 500));

      if (env.getPureConstantColumnShard(colName) != null) {
        long columnValueId = env.getPureConstantColumnShard(colName).getSingleColumnDictId();

        Map<Long, Long> rowIdToDictIdMap = new HashMap<>();
        for (Long curRowId : activeRowIds)
          rowIdToDictIdMap.put(curRowId, columnValueId);
        logger.trace("Resolving column dict IDs of col {} done, was easy as it was a constant col, sending out updates",
            colName);
        forEachOutputConsumerOfType(ColumnDictIdConsumer.class, c -> c.consume(env, colName, rowIdToDictIdMap));
      } else {
        Map<Long, Long> rowIdToColumnValueId = env.getColumnShard(colName).resolveColumnValueIdsForRows(activeRowIds);

        logger.trace("Resolving column dict IDs of col {} done, sending out updates (limit): {}", colName,
            Iterables.limit(rowIdToColumnValueId.entrySet(), 100));
        forEachOutputConsumerOfType(ColumnDictIdConsumer.class, c -> c.consume(env, colName, rowIdToColumnValueId));
      }

      processedRowIds.addAll(activeRowIds);
    }

    if (!intermediateRun && rowIdSourceIsEmpty.get() && rowIds.isEmpty() && newestAdjustedRowIds.isEmpty()) {
      forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
      doneProcessing();
    }
  }

  @Override
  public List<GenericConsumer> inputConsumers() {
    return Arrays.asList(new GenericConsumer[] { rowIdConsumer, colBuiltConsumer, columnVersionBuiltConsumer });
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof ColumnDictIdConsumer))
      throw new IllegalArgumentException("Only ColumnDictIdConsumer supported!");
  }

  @Override
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    if (rowIdConsumer.getNumberOfTimesWired() == 0)
      throw new ExecutablePlanBuildException("RowID consumer is not wired on " + this.toString());
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "colName=" + colName;
  }
}
