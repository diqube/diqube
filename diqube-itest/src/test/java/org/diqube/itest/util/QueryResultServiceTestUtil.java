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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.diqube.itest.util.ServerControl.ServerAddr;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.QueryResultServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryResultService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.remote.query.thrift.RQueryException;
import org.diqube.remote.query.thrift.RQueryStatistics;
import org.diqube.remote.query.thrift.RResultTable;

/**
 * Util class which opens a {@link QueryResultService} in this process in order to receive results from calling a
 * {@link QueryService} on a diqube-server.
 *
 * @author Bastian Gloeckle
 */
public class QueryResultServiceTestUtil {
  public static TestQueryResultService createQueryResultService() {
    short port = 5200; // TODO #38 find port dynamically.

    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    TestQueryResultService res = new TestQueryResultService(new ServerAddr("127.0.0.1", port));
    QueryResultServiceImpl serviceImpl = new QueryResultServiceImpl(res);

    multiProcessor.registerProcessor(QueryResultServiceConstants.SERVICE_NAME,
        new QueryResultService.Processor<QueryResultService.Iface>(serviceImpl));
    multiProcessor.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME,
        new KeepAliveService.Processor<KeepAliveService.Iface>(new KeepAliveService.Iface() {
          @Override
          public void ping() throws TException {
            // noop.
          }
        }));

    TServerSocket transport;
    try {
      transport = new TServerSocket(new InetSocketAddress("127.0.0.1", port));
    } catch (TTransportException e) {
      throw new RuntimeException("Could not open transport for result service", e);
    }
    TServer.Args args = new TServer.Args(transport);
    args.processor(multiProcessor);
    args.transportFactory(new TFramedTransport.Factory());
    args.protocolFactory(new TCompactProtocol.Factory());
    TSimpleServer thriftServer = new TSimpleServer(args);

    Thread serverThread = new Thread(() -> thriftServer.serve(), "Test-QueryResultService-serverthread");

    res.setThriftServer(thriftServer);
    serverThread.start();
    return res;
  }

  public static class TestQueryResultService implements Closeable {
    private TSimpleServer thriftServer;

    private Map<Short, RResultTable> intermediateUpdates = new ConcurrentHashMap<>();
    private RResultTable finalUpdate = null;
    private RQueryException exception = null;
    private RQueryStatistics stats = null;

    private ServerAddr thisServicesAddr;

    /* package */ TestQueryResultService(ServerAddr thisServicesAddr) {
      this.thisServicesAddr = thisServicesAddr;
    }

    /**
     * Checks if there was an exception in the meantime and throws it.
     */
    public boolean check() throws RuntimeException {
      if (exception != null)
        throw new RuntimeException("Exception while executing a query: " + exception.getMessage());
      return true;
    }

    @Override
    public void close() throws IOException {
      thriftServer.stop();
    }

    private void setThriftServer(TSimpleServer thriftServer) {
      this.thriftServer = thriftServer;
    }

    public Map<Short, RResultTable> getIntermediateUpdates() {
      return intermediateUpdates;
    }

    public RResultTable getFinalUpdate() {
      return finalUpdate;
    }

    public RQueryStatistics getStats() {
      return stats;
    }

    public ServerAddr getThisServicesAddr() {
      return thisServicesAddr;
    }
  }

  /**
   * Internal implementation of {@link QueryResultService} which forwards all data to {@link TestQueryResultService}.
   */
  private static class QueryResultServiceImpl implements QueryResultService.Iface {
    private TestQueryResultService res;

    public QueryResultServiceImpl(TestQueryResultService res) {
      this.res = res;
    }

    @Override
    public void partialUpdate(RUUID queryRUuid, RResultTable partialResult, short percentComplete) throws TException {
      res.intermediateUpdates.put(percentComplete, partialResult);
    }

    @Override
    public void queryResults(RUUID queryRUuid, RResultTable finalResult) throws TException {
      res.finalUpdate = finalResult;
    }

    @Override
    public void queryException(RUUID queryRUuid, RQueryException exceptionThrown) throws TException {
      res.exception = exceptionThrown;
    }

    @Override
    public void queryStatistics(RUUID queryRuuid, RQueryStatistics stats) throws TException {
      res.stats = stats;
    }
  }
}
