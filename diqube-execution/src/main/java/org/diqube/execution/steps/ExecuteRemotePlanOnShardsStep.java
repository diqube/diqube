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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFromRemoteBuilder;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.consumers.AbstractThreadedColumnValueConsumer;
import org.diqube.execution.consumers.AbstractThreadedGroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryUuid;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Executes a {@link RExecutionPlan} on all TableShards and provides the results of these executions. This is executed
 * on Query master node.
 * 
 * <p>
 * Input: None. <br>
 * Output: {@link ColumnValueConsumer}, {@link GroupIntermediaryAggregationConsumer}, {@link RowIdConsumer}.
 * 
 * @author Bastian Gloeckle
 */
public class ExecuteRemotePlanOnShardsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(ExecuteRemotePlanOnShardsStep.class);

  private RExecutionPlan remoteExecutionPlan;
  private RemotePlanBuilder remotePlanBuilder;

  private GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer =
      new AbstractThreadedGroupIntermediaryAggregationConsumer(null) {

        @Override
        protected void allSourcesAreDone() {
          forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class, c -> c.sourceIsDone());
        }

        @Override
        protected void doConsumeIntermediaryAggregationResult(long groupId, String colName,
            IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
            IntermediaryResult<Object, Object, Object> newIntermediaryResult) {
          logger.trace("Received intermediary results for group {} from remote", groupId);
          forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class, c -> c
              .consumeIntermediaryAggregationResult(groupId, colName, oldIntermediaryResult, newIntermediaryResult));
        }
      };

  private ColumnValueConsumer columnValueConsumer = new AbstractThreadedColumnValueConsumer(null) {
    private final Object EMPTY = new Object();

    private Map<Long, Object> alreadyReportedRowIds = new HashMap<>();

    @Override
    protected void allSourcesAreDone() {
      forEachOutputConsumerOfType(ColumnValueConsumer.class, c -> c.sourceIsDone());
      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.sourceIsDone());
    }

    @Override
    protected synchronized void doConsume(String colName, Map<Long, Object> values) {
      logger.trace("Received column values for col '{}' and rowIds (limit) {} from remote", colName,
          Iterables.limit(values.keySet(), 100));

      forEachOutputConsumerOfType(ColumnValueConsumer.class, c -> c.consume(colName, values));

      // feed data into RowIdConsumer
      Set<Long> newRowIds = new HashSet<>();
      for (Long rowId : values.keySet()) {
        // As we'll receive data for each row ID multiple times (at least for each column), we'll merge them here.
        if (alreadyReportedRowIds.put(rowId, EMPTY) == null)
          newRowIds.add(rowId);
      }
      Long[] newRowIdsArray = newRowIds.stream().toArray(l -> new Long[l]);
      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(newRowIdsArray));
    }
  };

  private ExecutorManager executorManager;

  public ExecuteRemotePlanOnShardsStep(int stepId, ExecutionEnvironment env, RExecutionPlan remoteExecutionPlan,
      ExecutablePlanFromRemoteBuilderFactory executablePlanFromRemoteBuilderFactory, ExecutorManager executorManager) {
    super(stepId);
    this.remoteExecutionPlan = remoteExecutionPlan;
    this.executorManager = executorManager;
    this.remotePlanBuilder = new DefaultRemotePlanBuilder(executablePlanFromRemoteBuilderFactory);
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof GroupIntermediaryAggregationConsumer) && !(consumer instanceof ColumnValueConsumer)
        && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException(
          "Only ColumnValueConsumer, RowIdConsumer and GroupIntermediaryAggregationConsumer supported.");
  }

  @Override
  protected void execute() {
    List<ExecutablePlan> remoteExecutablePlans =
        remotePlanBuilder.build(remoteExecutionPlan, groupIntermediaryAggregationConsumer, columnValueConsumer);

    // start execution on all TableShards
    // TODO #11 execute really on remote.
    int subCnt = 0;
    List<Future<Void>> futures = new ArrayList<>();
    for (ExecutablePlan executablePlanOnShard : remoteExecutablePlans) {
      Executor executor = executorManager.newQueryFixedThreadPool(executablePlanOnShard.preferredExecutorServiceSize(),
          Thread.currentThread().getName() + "-sub-" + (subCnt++) + "-%d", //
          QueryUuid.getCurrentQueryUuid(), QueryUuid.getCurrentExecutionUuid());

      Future<Void> future = executablePlanOnShard.executeAsynchronously(executor);
      futures.add(future);
    }

    // wait for all TableShards
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        // this will effectively end up stopping the execution of the whole ExecutablePlan. This is what we want. See
        // QueryUncaughtExceptionHandler.
        throw new ExecutablePlanExecutionException("Could not execute remote plan", e);
      }
    }

    // explicitly do NOT send sourceIsDone, as this is sent by the anonymous classes already.
    doneProcessing();
  }

  @Override
  protected List<GenericConsumer> inputConsumers() {
    return new ArrayList<>();
  }

  public RExecutionPlan getRemoteExecutionPlan() {
    return remoteExecutionPlan;
  }

  @Override
  protected String getAdditionalToStringDetails() {
    return "remoteExecutionPlan=" + remoteExecutionPlan;
  }

  /* package */void setRemotePlanBuilder(RemotePlanBuilder remotePlanBuilder) {
    this.remotePlanBuilder = remotePlanBuilder;
  }

  /**
   * Abstraction interface that can build {@link ExecutablePlan}s from a {@link RExecutionPlan}. Used for testing.
   */
  /* package */static interface RemotePlanBuilder {
    public List<ExecutablePlan> build(RExecutionPlan remotePlan,
        GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer,
        ColumnValueConsumer columnValueConsumer);
  }

  /* package */static class DefaultRemotePlanBuilder implements RemotePlanBuilder {

    private ExecutablePlanFromRemoteBuilderFactory executablePlanFromRemoteBuilderFactory;

    public DefaultRemotePlanBuilder(ExecutablePlanFromRemoteBuilderFactory executablePlanFromRemoteBuilderFactory) {
      this.executablePlanFromRemoteBuilderFactory = executablePlanFromRemoteBuilderFactory;
    }

    @Override
    public List<ExecutablePlan> build(RExecutionPlan remotePlan,
        GroupIntermediaryAggregationConsumer groupIntermediaryAggregationConsumer,
        ColumnValueConsumer columnValueConsumer) {
      ExecutablePlanFromRemoteBuilder builder =
          executablePlanFromRemoteBuilderFactory.createExecutablePlanFromRemoteBuilder();
      builder.withRemoteExecutionPlan(remotePlan);
      builder.withFinalColumnValueConsumer(columnValueConsumer);
      builder.withFinalGroupIntermediateAggregationConsumer(groupIntermediaryAggregationConsumer);

      return builder.build();
    }

  }

}
