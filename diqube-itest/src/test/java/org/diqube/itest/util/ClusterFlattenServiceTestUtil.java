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
package org.diqube.itest.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.diqube.connection.integrity.IntegrityCheckingProtocol;
import org.diqube.connection.integrity.RememberingTransport;
import org.diqube.itest.control.ServerControl.ServerAddr;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.ClusterFlattenServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.RFlattenException;
import org.diqube.remote.cluster.thrift.ROptionalUuid;
import org.diqube.remote.cluster.thrift.RRetryLaterException;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class which opens a {@link ClusterFlattenService} in this process in order to receive results from calling
 * another {@link ClusterFlattenService} on a diqube-server.
 *
 * @author Bastian Gloeckle
 */
public class ClusterFlattenServiceTestUtil {
  private static final Logger logger = LoggerFactory.getLogger(ClusterFlattenServiceTestUtil.class);

  public static TestClusterFlattenService createClusterFlattenService(byte[] serverMacKey) {
    short port = 5200; // TODO find port dynamically.

    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    TestClusterFlattenService res = new TestClusterFlattenService(new ServerAddr("127.0.0.1", port));
    ClusterFlattenServiceImpl serviceImpl = new ClusterFlattenServiceImpl(res);

    multiProcessor.registerProcessor(ClusterFlattenServiceConstants.SERVICE_NAME,
        new ClusterFlattenService.Processor<ClusterFlattenService.Iface>(serviceImpl));
    multiProcessor.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME,
        // no integrity check for keep alives.
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new KeepAliveService.Processor<KeepAliveService.Iface>(new KeepAliveService.Iface() {
              @Override
              public void ping() throws TException {
                // noop.
              }
            })));

    TNonblockingServerSocket transport;
    try {
      transport = new TNonblockingServerSocket(new InetSocketAddress("127.0.0.1", port));
    } catch (TTransportException e) {
      throw new RuntimeException("Could not open transport for result service", e);
    }
    TNonblockingServer.Args args = new TNonblockingServer.Args(transport);
    args.processor(multiProcessor);
    args.transportFactory(new RememberingTransport.Factory(new TFramedTransport.Factory()));
    args.protocolFactory(new IntegrityCheckingProtocol.Factory(new TCompactProtocol.Factory(), serverMacKey));
    TNonblockingServer thriftServer = new TNonblockingServer(args);

    Thread serverThread = new Thread(() -> thriftServer.serve(), "Test-ClusterFlattenService-serverthread");

    res.setThriftServer(thriftServer);
    res.setServerThread(serverThread);
    serverThread.start();
    return res;
  }

  public static class TestClusterFlattenService implements Closeable {
    private TServer thriftServer;
    private Thread serverThread;

    private Deque<RFlattenException> exceptions = new ConcurrentLinkedDeque<>();
    /** map from address of not to list of pair of "requestId" and "flattenId" that were received from the node */
    private ConcurrentMap<RNodeAddress, Deque<Pair<UUID, UUID>>> nodeResults = new ConcurrentHashMap<>();

    private ServerAddr thisServicesAddr;

    /* package */ TestClusterFlattenService(ServerAddr thisServicesAddr) {
      this.thisServicesAddr = thisServicesAddr;
    }

    /**
     * Checks if there was an exception in the meantime and throws it.
     */
    public boolean check() throws RuntimeException {
      if (!exceptions.isEmpty())
        throw new RuntimeException("Exceptions while executing a query: "
            + exceptions.stream().map(e -> e.getMessage()).collect(Collectors.toList()));
      return true;
    }

    @Override
    public void close() throws IOException {
      thriftServer.stop();
      try {
        serverThread.join(1000);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for test thread to shut down.", e);
      }
      if (serverThread.isAlive())
        throw new IOException("Could not shutdown test server thread.");
    }

    private void setThriftServer(TServer thriftServer) {
      this.thriftServer = thriftServer;
    }

    private void setServerThread(Thread serverThread) {
      this.serverThread = serverThread;
    }

    public ServerAddr getThisServicesAddr() {
      return thisServicesAddr;
    }

    public Deque<RFlattenException> getExceptions() {
      return exceptions;
    }

    public Map<RNodeAddress, Deque<Pair<UUID, UUID>>> getNodeResults() {
      return nodeResults;
    }

  }

  /**
   * Internal implementation of {@link QueryResultService} which forwards all data to {@link TestClusterFlattenService}.
   */
  private static class ClusterFlattenServiceImpl implements ClusterFlattenService.Iface {
    private TestClusterFlattenService res;

    public ClusterFlattenServiceImpl(TestClusterFlattenService res) {
      this.res = res;
    }

    @Override
    public void flattenAllLocalShards(RUUID flattenRequestId, String tableName, String flattenBy,
        List<RNodeAddress> otherFlatteners, RNodeAddress resultAddress) throws RFlattenException, TException {
      throw new RuntimeException("flattenAllLocalShards was called on the service instance of the test.");
    }

    @Override
    public void shardsFlattened(RUUID flattenRequestId, Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRowsDelta,
        RNodeAddress flattener) throws RRetryLaterException, TException {
      throw new RuntimeException("shardsFlattened was called on the service instance of the test.");
    }

    @Override
    public ROptionalUuid getLatestValidFlattening(String tableName, String flattenBy)
        throws RFlattenException, TException {
      throw new RuntimeException("getLatestValidFlattening was called on the service instance of the test.");
    }

    @Override
    public void flattenDone(RUUID flattenRequestId, RUUID flattenedTableId, RNodeAddress flattener) throws TException {
      Deque<Pair<UUID, UUID>> newList = new ConcurrentLinkedDeque<>();
      Deque<Pair<UUID, UUID>> previous = res.nodeResults.putIfAbsent(flattener, newList);

      if (previous != null)
        newList = previous;

      logger.info("Received flattenDone: request {}, tableId {}, flattener {}", flattenRequestId, flattenedTableId,
          flattener);
      newList.add(new Pair<>(RUuidUtil.toUuid(flattenRequestId), RUuidUtil.toUuid(flattenedTableId)));
    }

    @Override
    public void flattenFailed(RUUID flattenRequestId, RFlattenException flattenException) throws TException {
      logger.info("Received exception: request {}, exception {}", flattenRequestId, flattenException.getMessage(),
          flattenException);
      res.exceptions.add(flattenException);
    }
  }
}
