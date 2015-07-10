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
package org.diqube.server;

import javax.inject.Inject;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.cluster.ClusterNodeServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterNodeService;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.server.config.Config;
import org.diqube.server.config.ConfigKey;
import org.diqube.server.thrift.ThriftServer;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a thrift server and serves any incoming connections.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ServerImplementation {

  private static final Logger logger = LoggerFactory.getLogger(ServerImplementation.class);

  @Config(ConfigKey.PORT)
  private int port;

  @Config(ConfigKey.SELECTOR_THREADS)
  private int selectorThreads;

  @Inject
  private ClusterNodeService.Iface clusterNodeHandler;

  @Inject
  private QueryService.Iface queryHandler;

  @Inject
  private ExecutorManager executorManager;

  private TThreadedSelectorServer server;

  public void serve() {
    TThreadedSelectorServer.Args serverArgs = createServerArgs(clusterNodeHandler, queryHandler);

    if (serverArgs == null)
      return;

    server = new ThriftServer(serverArgs, "server-selector-%d");

    // Make sure we at least try to clean up a little bit when the VM is shut down.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        logger.info("Shutting down server...");
        server.stop();
        executorManager.shutdownEverythingOfAllQueries();
      }
    }, "shutdown-thread"));

    logger.info("Listening for incoming requests on port {}...", port);
    server.serve();
  }

  private TThreadedSelectorServer.Args createServerArgs(ClusterNodeService.Iface clusterNodeHandler,
      QueryService.Iface queryHandler) {
    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    multiProcessor.registerProcessor(QueryServiceConstants.SERVICE_NAME,
        new QueryService.Processor<QueryService.Iface>(queryHandler));
    multiProcessor.registerProcessor(ClusterNodeServiceConstants.SERVICE_NAME,
        new ClusterNodeService.Processor<ClusterNodeService.Iface>(clusterNodeHandler));

    TNonblockingServerTransport transport;
    try {
      transport = new TNonblockingServerSocket(port);
    } catch (TTransportException e) {
      logger.error("Could not bind to port {}", port, e);
      return null;
    }

    // TThreadedSelectorServer:
    // 1 Accept Thread
    // selectorThreads number of selector threads: Read and write for accepted connections
    // uses ExecutorService to actually invoke any methods.
    TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(transport);
    serverArgs.processor(multiProcessor);
    serverArgs.transportFactory(new TFramedTransport.Factory());
    serverArgs.protocolFactory(new TCompactProtocol.Factory());
    logger.info("Thrift server will use {} selector threads.", selectorThreads);
    serverArgs.selectorThreads(selectorThreads);
    serverArgs.executorService(executorManager.newCachedThreadPool("server-worker-%d", new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        logger.error("Uncaught exception in one of the server workers", e);
        server.stop(); // stop everything and shut down.
      }
    }));
    return serverArgs;
  }
}
