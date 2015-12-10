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
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.diqube.connection.integrity.IntegrityCheckingProtocol;
import org.diqube.itest.control.ServerControl.ServerAddr;
import org.diqube.remote.query.IdentityCallbackServiceConstants;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.TicketInfo;
import org.diqube.thriftutil.RememberingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class which opens a {@link IdentityCallbackService} in this process in order to receive results from calling
 * another {@link IdentityService} on a diqube-server.
 *
 * @author Bastian Gloeckle
 */
public class IdentityCallbackServiceTestUtil {
  private static final Logger logger = LoggerFactory.getLogger(IdentityCallbackServiceTestUtil.class);

  public static TestIdentityCallbackService createIdentityCallbackService() {
    short port = 5200; // TODO find port dynamically.

    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    TestIdentityCallbackService res = new TestIdentityCallbackService(new ServerAddr("127.0.0.1", port));
    IdentityCallbackServiceImpl serviceImpl = new IdentityCallbackServiceImpl(res);

    multiProcessor.registerProcessor(IdentityCallbackServiceConstants.SERVICE_NAME,
        new IdentityCallbackService.Processor<IdentityCallbackService.Iface>(serviceImpl));
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
    args.protocolFactory(new TCompactProtocol.Factory());
    TNonblockingServer thriftServer = new TNonblockingServer(args);

    Thread serverThread = new Thread(() -> thriftServer.serve(), "Test-IdentityCallbackService-serverthread");

    res.setThriftServer(thriftServer);
    res.setServerThread(serverThread);
    serverThread.start();
    return res;
  }

  public static class TestIdentityCallbackService implements Closeable {
    private TServer thriftServer;
    private Thread serverThread;

    private Deque<TicketInfo> invalidTickets = new ConcurrentLinkedDeque<>();

    private ServerAddr thisServicesAddr;

    /* package */ TestIdentityCallbackService(ServerAddr thisServicesAddr) {
      this.thisServicesAddr = thisServicesAddr;
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

    public Deque<TicketInfo> getInvalidTickets() {
      return invalidTickets;
    }

  }

  /**
   * Internal implementation of {@link IdentityCallbackService} which forwards all data to
   * {@link TestIdentityCallbackService}.
   */
  private static class IdentityCallbackServiceImpl implements IdentityCallbackService.Iface {
    private TestIdentityCallbackService res;

    public IdentityCallbackServiceImpl(TestIdentityCallbackService res) {
      this.res = res;
    }

    @Override
    public void ticketBecameInvalid(TicketInfo ticketInfo) throws TException {
      logger.info("Test IdentityCallbackService received info for invalid ticket: {}", ticketInfo);
      res.invalidTickets.add(ticketInfo);
    }

  }
}
