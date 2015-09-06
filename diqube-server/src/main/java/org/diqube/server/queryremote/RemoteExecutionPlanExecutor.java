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
package org.diqube.server.queryremote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.data.TableShard;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFromRemoteBuilder;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.ExecutionPercentage;
import org.diqube.execution.TableRegistry;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupIntermediaryAggregationConsumer;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryRegistry;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.remote.cluster.RIntermediateAggregationResultUtil;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.cluster.thrift.ROldNewIntermediateAggregationResult;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a {@link RExecutionPlan} on a "query remote" node.
 * 
 * One instance of this class can only be used to execute one {@link RExecutionPlan} on the node.
 * 
 * <p>
 * As soon as result data is available, it will be published to the callback instance.
 * 
 * @author Bastian Gloeckle
 */
public class RemoteExecutionPlanExecutor {
  private static final Logger logger = LoggerFactory.getLogger(RemoteExecutionPlanExecutor.class);

  private ExecutablePlanFromRemoteBuilderFactory executablePlanBuilderFactory;

  private AtomicInteger previousPercentDone = new AtomicInteger(0);

  private ExecutorManager executorManager;

  private QueryRegistry queryRegistry;

  public RemoteExecutionPlanExecutor(TableRegistry tableRegistry,
      ExecutablePlanFromRemoteBuilderFactory executablePlanBuilderFactory, ExecutorManager executorManager,
      QueryRegistry queryRegistry) {
    this.executablePlanBuilderFactory = executablePlanBuilderFactory;
    this.executorManager = executorManager;
    this.queryRegistry = queryRegistry;
  }

  private short calculatePercentDoneDelta(List<ExecutionPercentage> executionPercentages) {
    int sum = 0;
    for (ExecutionPercentage perc : executionPercentages)
      sum += perc.calculatePercentDone();
    short newPercentDone = (short) (sum / executionPercentages.size());

    short previousValue = (short) previousPercentDone.getAndAccumulate(newPercentDone, (a, b) -> Math.max(a, b));

    if (previousValue >= newPercentDone)
      return (short) 0;

    return (short) (newPercentDone - previousValue);
  }

  /**
   * Prepares execution of the given {@link RExecutionPlan}.
   * 
   * The returned {@link Runnable} will, when {@link Runnable#run() ran}, start execution of the plan on all local
   * {@link TableShard}s and will block the current thread until the execution is complete. Until then, the provided
   * callback will be called accordingly.
   * 
   * For each TableShard a new Executor will be used and a corresponding execution UUID will be created. This will not
   * automatically be registered with {@link QueryRegistry} though!
   * 
   * @return Pair of runnable (see above) and the ExecutablePlans that were created from the {@link RExecutionPlan} and
   *         will be executed. <code>null</code> will be returned in case there is nothing to execute (e.g. the
   *         TableShards of the Table were just unloaded).
   */
  public Pair<Runnable, List<ExecutablePlan>> prepareExecution(UUID queryUuid, UUID executionUuid,
      RExecutionPlan executionPlan, RemoteExecutionPlanExecutionCallback callback) {
    Holder<List<ExecutionPercentage>> executionPercentageHolder = new Holder<>();

    ExecutablePlanFromRemoteBuilder executablePlanBuilder =
        executablePlanBuilderFactory.createExecutablePlanFromRemoteBuilder();
    executablePlanBuilder.withRemoteExecutionPlan(executionPlan);
    executablePlanBuilder.withFinalColumnValueConsumer(new AbstractThreadedColumnValueConsumer(null) {
      @Override
      protected void allSourcesAreDone() {
      }

      @Override
      protected void doConsume(String colName, Map<Long, Object> values) {
        Map<Long, RValue> res = new HashMap<>();
        for (Entry<Long, Object> inputEntry : values.entrySet())
          res.put(inputEntry.getKey(), RValueUtil.createRValue(inputEntry.getValue()));

        callback.newColumnValues(colName, res, calculatePercentDoneDelta(executionPercentageHolder.getValue()));
      }
    });
    executablePlanBuilder
        .withFinalGroupIntermediateAggregationConsumer(new AbstractThreadedGroupIntermediaryAggregationConsumer(null) {
          @Override
          protected void allSourcesAreDone() {
          }

          @Override
          protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
              IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
              IntermediaryResult<Object, Object, Object> newIntermediaryResult) {
            ROldNewIntermediateAggregationResult res = new ROldNewIntermediateAggregationResult();
            if (oldIntermediaryResult != null)
              res.setOldResult(
                  RIntermediateAggregationResultUtil.buildRIntermediateAggregationResult(oldIntermediaryResult));
            if (newIntermediaryResult != null)
              res.setNewResult(
                  RIntermediateAggregationResultUtil.buildRIntermediateAggregationResult(newIntermediaryResult));

            callback.newGroupIntermediaryAggregration(groupId, colName, res,
                calculatePercentDoneDelta(executionPercentageHolder.getValue()));
          }
        });
    List<ExecutablePlan> executablePlans = executablePlanBuilder.build();

    if (executablePlans.size() == 0) {
      logger.info(
          "Could not identify any local executable plans for query {} execution {}. Probably the TableShards were just unloaded.",
          queryUuid, executionUuid);
      return null;
    }

    List<ExecutionPercentage> executionPercentages = new ArrayList<>();
    for (ExecutablePlan plan : executablePlans) {
      ExecutionPercentage newPercentage = new ExecutionPercentage(plan);
      newPercentage.attach();
      executionPercentages.add(newPercentage);
    }
    executionPercentageHolder.setValue(executionPercentages);

    return new Pair<>(new Runnable() {
      @Override
      public void run() {
        List<Future<?>> futures = new ArrayList<>();

        int numberOfThreads = 0;
        for (ExecutablePlan plan : executablePlans) {
          TableShard shard = plan.getDefaultExecutionEnvironment().getTableShardIfAvailable();

          numberOfThreads += plan.preferredExecutorServiceSize();

          Executor executor = executorManager.newQueryFixedThreadPool(plan.preferredExecutorServiceSize(),
              "query-remote-worker-" + queryUuid + "-shard" + shard.getLowestRowId() + "-%d", queryUuid, executionUuid);

          Future<Void> f = plan.executeAsynchronously(executor);
          futures.add(f);
        }

        queryRegistry.getOrCreateCurrentStatsManager().setNumberOfThreads(numberOfThreads);

        for (Future<?> f : futures)
          try {
            f.get();
          } catch (ExecutionException e) {
            // swallow, the exception will be handled by the "unhandedExceptionHandler" of the steps thread.
            return;
          } catch (InterruptedException e) {
            // interrupted, stop quietly.
            return;
          }
        callback.executionDone();
      }
    }, executablePlans);
  }

  /**
   * Callback for new data that was calculated for any TableShard.
   */
  public static interface RemoteExecutionPlanExecutionCallback {
    /**
     * Execution on all local TableShards has completed.
     */
    public void executionDone();

    /**
     * New column values are available.
     */
    public void newColumnValues(String colName, Map<Long, RValue> values, short percentDone);

    /**
     * A new intermediary result from an aggregation function is available.
     */
    public void newGroupIntermediaryAggregration(long groupId, String colName,
        ROldNewIntermediateAggregationResult result, short percentDone);
  }

}
