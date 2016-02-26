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
package org.diqube.consensus.internal;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.consensus.ConsensusServer;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.shutdown.ContextShutdownListener;
import org.diqube.context.shutdown.ShutdownAfter;
import org.diqube.remote.cluster.thrift.ClusterConsensusService;
import org.diqube.remote.cluster.thrift.RConnectionUnknownException;
import org.diqube.thrift.base.thrift.RNodeAddress;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.util.RUuidUtil;

import io.atomix.catalyst.util.concurrent.CatalystThreadFactory;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadPoolContext;

/**
 * Handler for {@link ClusterConsensusService} which handles communication of copycat traffic for our consensus.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterConsensusHandler implements ClusterConsensusService.Iface, ContextShutdownListener {

  @Inject
  private ClusterConsensusConnectionRegistry registry;

  @Inject
  private DiqubeCatalystConnectionFactory factory;

  @Inject
  private DiqubeCatalystServer server;

  @Inject
  private DiqubeCatalystSerializer serializer;

  private volatile ThreadContext openConnectionsThreadContext;

  private Deque<Runnable> cleanupActions = new ConcurrentLinkedDeque<>();

  @Override
  @ShutdownAfter(ConsensusServer.class)
  public void contextAboutToShutdown() {
    while (!cleanupActions.isEmpty())
      cleanupActions.poll().run();
  }

  /**
   * Open a new catalyst connection
   */
  @Override
  public RUUID open(RUUID otherConsensusConnectionEndpointId, RNodeAddress resultAddress) throws TException {
    DiqubeCatalystConnection newCon = factory.createDiqubeCatalystConnection(getOpenConnectionsThreadContext());
    UUID thisConnectionEndpointId =
        newCon.acceptAndRegister(RUuidUtil.toUuid(otherConsensusConnectionEndpointId), resultAddress);
    server.newClientConnection(newCon);

    return RUuidUtil.toRUuid(thisConnectionEndpointId);
  }

  /**
   * Close a catalyst connection
   */
  @Override
  public void close(RUUID consensusConnectionEndpointId) throws RConnectionUnknownException, TException {
    DiqubeCatalystConnection con = registry.getConnectionEndpoint(RUuidUtil.toUuid(consensusConnectionEndpointId));
    if (con != null)
      con.close();
    else
      throw new RConnectionUnknownException(
          "Consensus connection endpoint unknown: " + RUuidUtil.toUuid(consensusConnectionEndpointId).toString());
  }

  /**
   * Send a "request" of an opened catalyst connection
   */
  @Override
  public void request(RUUID consensusConnectionEndpointId, RUUID consensusRequestId, ByteBuffer data)
      throws RConnectionUnknownException, TException {
    DiqubeCatalystConnection con = registry.getConnectionEndpoint(RUuidUtil.toUuid(consensusConnectionEndpointId));
    if (con != null)
      con.handleRequest(RUuidUtil.toUuid(consensusRequestId), data);
    else
      throw new RConnectionUnknownException(
          "Consensus connection endpoint unknown: " + RUuidUtil.toUuid(consensusConnectionEndpointId).toString());
  }

  /**
   * Send a "response" (to a request which was received before) of an opened catalyst connection
   */
  @Override
  public void reply(RUUID consensusConnectionEndpointId, RUUID consensusRequestId, ByteBuffer data)
      throws RConnectionUnknownException, TException {
    DiqubeCatalystConnection con = registry.getConnectionEndpoint(RUuidUtil.toUuid(consensusConnectionEndpointId));
    if (con != null)
      con.handleResponse(RUuidUtil.toUuid(consensusRequestId), data);
    else
      throw new RConnectionUnknownException(
          "Consensus connection endpoint unknown: " + RUuidUtil.toUuid(consensusConnectionEndpointId).toString());
  }

  /**
   * Send an "exceptional response" (to a request which was received before) of an opened catalyst connection
   */
  @Override
  public void replyException(RUUID consensusConnectionEndpointId, RUUID consensusRequestId, ByteBuffer data)
      throws RConnectionUnknownException, TException {
    DiqubeCatalystConnection con = registry.getConnectionEndpoint(RUuidUtil.toUuid(consensusConnectionEndpointId));
    if (con != null)
      con.handleResponseException(RUuidUtil.toUuid(consensusRequestId), data);
    else
      throw new RConnectionUnknownException(
          "Consensus connection endpoint unknown: " + RUuidUtil.toUuid(consensusConnectionEndpointId).toString());
  }

  /**
   * @return A Catalyst {@link ThreadContext} that can handle executing things for connections that were opened by other
   *         hosts.
   */
  private ThreadContext getOpenConnectionsThreadContext() {
    if (openConnectionsThreadContext == null)
      synchronized (this) {
        if (openConnectionsThreadContext == null) {
          ScheduledExecutorService executorService =
              Executors.newScheduledThreadPool(10, new CatalystThreadFactory("diqube-copycat-connection-%d"));
          openConnectionsThreadContext = new ThreadPoolContext(executorService, serializer);
          cleanupActions.add(() -> executorService.shutdownNow());
        }
      }
    return openConnectionsThreadContext;

  }

}
