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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.ConnectionPool.Connection;
import org.diqube.cluster.connection.ConnectionPool.ConnectionException;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFromRemoteBuilder;
import org.diqube.execution.ExecutablePlanFromRemoteBuilderFactory;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.consumers.RowIdConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanExecutionException;
import org.diqube.function.IntermediaryResult;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryRegistry.QueryResultHandler;
import org.diqube.queries.QueryUuid;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.ClusterQueryServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
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
  private ClusterManager clusterManager;
  private QueryRegistry queryRegistry;
  private ConnectionPool connectionPool;
  private ClusterQueryService.Iface localClusterQueryService;

  private Object wait = new Object();

  private String exceptionMessage = null;
  private AtomicInteger remotesDone = new AtomicInteger(0);

  private QueryResultHandler resultHandler = new QueryResultHandler() {
    private final Object EMPTY = new Object();

    private Map<Long, Object> alreadyReportedRowIds = new HashMap<>();

    @Override
    public void oneRemoteException(String msg) {
      exceptionMessage = msg;
      logger.trace("One remote is exception");
      synchronized (wait) {
        wait.notifyAll();
      }
    }

    @Override
    public void oneRemoteDone() {
      remotesDone.incrementAndGet();
      logger.trace("One remote is done");
      synchronized (wait) {
        wait.notifyAll();
      }
    }

    @Override
    public void newIntermediaryAggregationResult(long groupId, String colName,
        IntermediaryResult<Object, Object, Object> oldIntermediaryResult,
        IntermediaryResult<Object, Object, Object> newIntermediaryResult) {
      logger.trace("Received intermediary results for group {} from remote", groupId);
      forEachOutputConsumerOfType(GroupIntermediaryAggregationConsumer.class,
          c -> c.consumeIntermediaryAggregationResult(groupId, colName, oldIntermediaryResult, newIntermediaryResult));
    }

    @Override
    public void newColumnValues(String colName, Map<Long, Object> values) {
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

  public ExecuteRemotePlanOnShardsStep(int stepId, ExecutionEnvironment env, RExecutionPlan remoteExecutionPlan,
      ClusterManager clusterManager, QueryRegistry queryRegistry, ConnectionPool connectionPool,
      ClusterQueryService.Iface localClusterQueryService) {
    super(stepId);
    this.remoteExecutionPlan = remoteExecutionPlan;
    this.clusterManager = clusterManager;
    this.queryRegistry = queryRegistry;
    this.connectionPool = connectionPool;
    this.localClusterQueryService = localClusterQueryService;
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
    Collection<RNodeAddress> remoteNodes =
        clusterManager.getClusterLayout().findNodesServingTable(remoteExecutionPlan.getTable());

    if (remoteNodes.isEmpty())
      throw new ExecutablePlanExecutionException(
          "There are no cluster nodes serving table '" + remoteExecutionPlan.getTable() + "'");

    int numberOfActiveRemotes = remoteNodes.size();
    queryRegistry.addQueryResultHandler(QueryUuid.getCurrentQueryUuid(), resultHandler);
    try {
      // distribute query execution
      RNodeAddress ourRemoteAddr = clusterManager.getOurHostAddr().createRemote();
      for (RNodeAddress remoteAddr : remoteNodes) {
        if (remoteAddr.equals(ourRemoteAddr)) {
          // short-cut in case the remote is actually local - do not de-/searialize and use network interface. This is
          // a nice implementation for unit tests, too.
          try {
            localClusterQueryService.executeOnAllLocalShards(remoteExecutionPlan,
                RUuidUtil.toRUuid(QueryUuid.getCurrentQueryUuid()), ourRemoteAddr);
          } catch (TException e) {
            logger.error("Could not execute remote plan on local node", e);
            throw new ExecutablePlanExecutionException("Could not execute remote plan on local node", e);
          }
          continue;
        }

        try (Connection<ClusterQueryService.Client> conn = connectionPool.reserveConnection(
            ClusterQueryService.Client.class, ClusterQueryServiceConstants.SERVICE_NAME, remoteAddr)) {

          conn.getService().executeOnAllLocalShards(remoteExecutionPlan,
              RUuidUtil.toRUuid(QueryUuid.getCurrentQueryUuid()), ourRemoteAddr);
        } catch (IOException | ConnectionException | TException e) {
          // TODO #32 mark connection as dead
          logger.error("Could not distribute execution of query {} to remote {}", QueryUuid.getCurrentQueryUuid(),
              remoteAddr, e);
          throw new ExecutablePlanExecutionException("Could not distribute query to cluster", e);
        }
      }

      // wait until done
      while (remotesDone.get() < numberOfActiveRemotes && exceptionMessage == null) {
        synchronized (wait) {
          try {
            wait.wait(1000);
          } catch (InterruptedException e) {
            // we were interrupted, exit quietly.
            return;
          }
        }
      }
    } finally {
      queryRegistry.removeQueryResultHandler(QueryUuid.getCurrentQueryUuid(), resultHandler);
    }

    if (exceptionMessage != null)
      throw new ExecutablePlanExecutionException(
          "Exception while waiting for the results from remotes: " + exceptionMessage);

    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
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
