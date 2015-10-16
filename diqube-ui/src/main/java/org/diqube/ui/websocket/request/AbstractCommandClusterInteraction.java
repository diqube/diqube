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
package org.diqube.ui.websocket.request;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RNodeHttpAddress;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.query.ClusterInformationServiceConstants;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.ClusterInformationService;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryResultService.Iface;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.ui.DiqubeServletConfig;
import org.diqube.ui.ThriftServlet;
import org.diqube.ui.UiQueryRegistry;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base implementation for {@link CommandClusterInteraction}.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractCommandClusterInteraction implements CommandClusterInteraction {
  private static final Logger logger = LoggerFactory.getLogger(AbstractCommandClusterInteraction.class);

  private DiqubeServletConfig config;

  /* package */ AbstractCommandClusterInteraction(DiqubeServletConfig config) {
    this.config = config;
  }

  @Override
  public void executeDiqlQuery(String diql, Iface resultHandler) throws RuntimeException {
    Set<Integer> idxToCheck = IntStream.range(0, config.getClusterServers().size()).boxed().collect(Collectors.toSet());

    UUID queryUuid = null;
    while (queryUuid == null) {
      if (idxToCheck.isEmpty())
        throw new RuntimeException("No cluster servers were reachable");
      int nextIdx = (int) Math.floor(Math.random() * config.getClusterServers().size());
      if (!idxToCheck.remove(nextIdx))
        continue;

      queryUuid = sendDiqlQuery(config.getClusterServers().get(nextIdx), diql, resultHandler);
    }
  }

  @Override
  public void cancelQuery() {
    Pair<UUID, Pair<String, Short>> queryUuidAndAddrPair = findQueryUuidAndServerAddr();

    UUID queryUuid = queryUuidAndAddrPair.getLeft();
    // server that the query was sent to. That is the query master for that query!
    Pair<String, Short> serverAddr = queryUuidAndAddrPair.getRight();

    TTransport transport = new TSocket(serverAddr.getLeft(), serverAddr.getRight());
    transport = new TFramedTransport(transport);
    TProtocol queryProtocol =
        new TMultiplexedProtocol(new TCompactProtocol(transport), QueryServiceConstants.SERVICE_NAME);

    QueryService.Client queryClient = new QueryService.Client(queryProtocol);
    try {
      transport.open();

      logger.info("Sending request to cancel query {} to the diqube server at {}.", queryUuid, serverAddr);
      queryClient.cancelQueryExecution(RUuidUtil.toRUuid(queryUuid));
    } catch (TException e) {
      logger.warn("Could not cancel execution of query {} although requested by user.", queryUuidAndAddrPair);
    } finally {
      transport.close();
    }
  }

  @Override
  public ClusterInformationService.Iface getClusterInformationService() {
    Set<Integer> idxToCheck = IntStream.range(0, config.getClusterServers().size()).boxed().collect(Collectors.toSet());

    ClusterInformationService.Client client = null;
    while (client == null) {
      if (idxToCheck.isEmpty())
        throw new RuntimeException("No cluster servers were reachable");
      int nextIdx = (int) Math.floor(Math.random() * config.getClusterServers().size());
      if (!idxToCheck.remove(nextIdx))
        continue;

      client = openConnection(ClusterInformationService.Client.class, ClusterInformationServiceConstants.SERVICE_NAME,
          config.getClusterServers().get(nextIdx));
    }

    return client;
  }

  private UUID sendDiqlQuery(Pair<String, Short> node, String diql, QueryResultService.Iface resultHandler) {
    QueryService.Iface queryClient =
        openConnection(QueryService.Client.class, QueryServiceConstants.SERVICE_NAME, node);

    if (queryClient == null)
      return null;

    UUID queryUuid = UUID.randomUUID();
    try {
      registerQueryThriftResultCallback(node, queryUuid, resultHandler);

      queryClient.asyncExecuteQuery(RUuidUtil.toRUuid(queryUuid), //
          diql, //
          true, createOurAddress());
      logger.info("Started executing new query {} on server {}", queryUuid, node);
      return queryUuid;
    } catch (RQueryException e) {
      throw new RuntimeException(e.getMessage());
    } catch (TException e) {
      return null;
    }
  }

  private RNodeAddress createOurAddress() {
    RNodeAddress res = new RNodeAddress();
    res.setHttpAddr(new RNodeHttpAddress());
    res.getHttpAddr().setUrl(config.getClusterResponseAddr() + ThriftServlet.URL_PATTERN);
    return res;
  }

  /**
   * Register a new query execution on the diqube cluster in {@link UiQueryRegistry}.
   * 
   * <p>
   * Note that the implementing class needs to take care of cleaning up the thrift result callback!
   * 
   * @param node
   *          Cluster node that the query is sent to - this is the query master!
   * @param queryUuid
   *          The UUID that was passed to the server as query UUID
   * @param resultHandler
   *          The handler for results of this query.
   */
  protected abstract void registerQueryThriftResultCallback(Pair<String, Short> node, UUID queryUuid,
      QueryResultService.Iface resultHandler);

  /**
   * @return Pair of query UUID and Server addr that the currently running query was sent to.
   */
  protected abstract Pair<UUID, Pair<String, Short>> findQueryUuidAndServerAddr();

  /**
   * Open a thift connection to a diqube-server.
   * 
   * <p>
   * Note that the implementing class needs to take care of closing the connection again!
   * 
   * @param thriftClientClass
   *          The "Client" class of the thrift service that a connection should be opened to.
   * @param serviceName
   *          The multiplexing name of the service.
   * @param node
   *          The node to open a connection to.
   * @return A service that can be called or <code>null</code> if connection could not be opened.
   */
  protected abstract <T extends TServiceClient> T openConnection(Class<? extends T> thriftClientClass,
      String serviceName, Pair<String, Short> node);

}
