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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.diqube.cluster.ClusterManager;
import org.diqube.cluster.connection.Connection;
import org.diqube.cluster.connection.ConnectionException;
import org.diqube.cluster.connection.ConnectionPool;
import org.diqube.cluster.connection.SocketListener;
import org.diqube.execution.RemotesTriggeredListener;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.DoneConsumer;
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
 * This step will install a {@link QueryResultHandler} with {@link QueryRegistry} to receive the results. It relies on
 * that the {@link QueryResultHandler#oneRemoteDone()} for a single remote is only called after all other calls of this
 * remote to other methods of the {@link QueryResultHandler} have returned already.
 * 
 * <p>
 * Input: None. <br>
 * Output: {@link ColumnValueConsumer}, {@link GroupIntermediaryAggregationConsumer}, {@link RowIdConsumer}.
 * 
 * @author Bastian Gloeckle
 */
public class ExecuteRemotePlanOnShardsStep extends AbstractThreadedExecutablePlanStep {

  private static final Logger logger = LoggerFactory.getLogger(ExecuteRemotePlanOnShardsStep.class);

  private List<RemotesTriggeredListener> remotesTriggeredListeners = new ArrayList<>();

  private RExecutionPlan remoteExecutionPlan;
  private ClusterManager clusterManager;
  private QueryRegistry queryRegistry;
  private ConnectionPool connectionPool;
  private ClusterQueryService.Iface localClusterQueryService;

  private Object wait = new Object();

  private Collection<RNodeAddress> remotesActive = null;

  private String exceptionMessage = null;
  private AtomicInteger remotesDone = new AtomicInteger(0);

  private QueryResultHandler resultHandler = new QueryResultHandler() {
    private Set<Long> alreadyReportedRowIds = new ConcurrentSkipListSet<>();

    @Override
    public void oneRemoteException(String msg) {
      exceptionMessage = (msg == null) ? "null" : msg; // msg may be null, but we know there was an exception (perhaps
                                                       // NPE?)
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
        if (alreadyReportedRowIds.add(rowId))
          newRowIds.add(rowId);
      }
      Long[] newRowIdsArray = newRowIds.stream().toArray(l -> new Long[l]);
      forEachOutputConsumerOfType(RowIdConsumer.class, c -> c.consume(newRowIdsArray));
    }
  };

  private int numberOfRemotesInformed;

  public ExecuteRemotePlanOnShardsStep(int stepId, QueryRegistry queryRegistry, ExecutionEnvironment env,
      RExecutionPlan remoteExecutionPlan, ClusterManager clusterManager, ConnectionPool connectionPool,
      ClusterQueryService.Iface localClusterQueryService) {
    super(stepId, queryRegistry);
    this.remoteExecutionPlan = remoteExecutionPlan;
    this.clusterManager = clusterManager;
    this.queryRegistry = queryRegistry;
    this.connectionPool = connectionPool;
    this.localClusterQueryService = localClusterQueryService;
  }

  @Override
  protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException {
    if (!(consumer instanceof DoneConsumer) && !(consumer instanceof GroupIntermediaryAggregationConsumer)
        && !(consumer instanceof ColumnValueConsumer) && !(consumer instanceof RowIdConsumer))
      throw new IllegalArgumentException(
          "Only ColumnValueConsumer, RowIdConsumer and GroupIntermediaryAggregationConsumer supported.");
  }

  @Override
  protected void execute() {
    Collection<RNodeAddress> remoteNodes =
        clusterManager.getClusterLayout().findNodesServingTable(remoteExecutionPlan.getTable());

    remotesActive = new ConcurrentLinkedDeque<>();

    if (remoteNodes.isEmpty())
      throw new ExecutablePlanExecutionException(
          "There are no cluster nodes serving table '" + remoteExecutionPlan.getTable() + "'");

    // this will be installed on the sockets we use to communicate to the remotes.
    SocketListener socketListener = new SocketListener() {
      @Override
      public void connectionDied() {
        // one remote won't be able to fulfill our request :/
        remotesDone.incrementAndGet();
        // TODO #37: Warn user.
        logger.warn(
            "A remote node died, but it would have contained information for query "
                + "{} execution {}. The information will not be available to the user therefore.",
            QueryUuid.getCurrentQueryUuid(), QueryUuid.getCurrentExecutionUuid());
      }
    };

    numberOfRemotesInformed = remoteNodes.size();
    remotesTriggeredListeners.forEach(l -> l.numberOfRemotesTriggered(numberOfRemotesInformed));

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
            ClusterQueryService.Client.class, ClusterQueryServiceConstants.SERVICE_NAME, remoteAddr, socketListener)) {

          conn.getService().executeOnAllLocalShards(remoteExecutionPlan,
              RUuidUtil.toRUuid(QueryUuid.getCurrentQueryUuid()), ourRemoteAddr);
          remotesActive.add(remoteAddr);
        } catch (IOException | ConnectionException | TException e) {
          // Connection will be marked as dead automatically, the remote will automatically be removed from ClusterNode
          // (see ConnectionPool). We just ignore the remote for now.
          logger.warn(
              "Remote node {} is not online anymore, but it would have contained information for query "
                  + "{} execution {}. The information will not be available to the user therefore.",
              remoteAddr, QueryUuid.getCurrentQueryUuid(), QueryUuid.getCurrentExecutionUuid());
          remotesDone.incrementAndGet();
          // TODO #37: We should inform the user about this situation.
        } catch (InterruptedException e) {
          logger.trace("Interrupted while waiting for a new connection.");
          doneProcessing();
          return;
        }
      }

      // wait until done
      while (remotesDone.get() < numberOfRemotesInformed && exceptionMessage == null) {
        synchronized (wait) {
          try {
            wait.wait(1000);
          } catch (InterruptedException e) {
            logger.trace("Interrupted while waiting for remotes.");
            doneProcessing();
            return;
          }
        }
      }
      remotesActive.clear();
    } finally {
      queryRegistry.removeQueryResultHandler(QueryUuid.getCurrentQueryUuid(), resultHandler);
    }

    if (exceptionMessage != null)
      throw new ExecutablePlanExecutionException(
          "Exception while waiting for the results from remotes: " + exceptionMessage);

    forEachOutputConsumerOfType(GenericConsumer.class, c -> c.sourceIsDone());
    doneProcessing();
  }

  /**
   * @return The addresses of those query remotes that this step triggered an execution on. Might be <code>null</code>
   *         or empty. Only valid before this step is done.
   */
  public Collection<RNodeAddress> getRemotesActive() {
    return remotesActive;
  }

  public int getNumberOfRemotesTriggerdOverall() {
    return numberOfRemotesInformed;
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

  @Override
  public String getDetailsDescription() {
    return null;
  }

  public void addRemotesTriggeredListener(RemotesTriggeredListener listener) {
    remotesTriggeredListeners.add(listener);
  }
}
