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

import java.net.InetSocketAddress;
import java.util.List;

import javax.inject.Inject;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.listeners.ServingListener;
import org.diqube.remote.cluster.ClusterManagementServiceConstants;
import org.diqube.remote.cluster.ClusterQueryServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryService;
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

  @Config(ConfigKey.BIND)
  private String bind;

  @Inject
  private ClusterQueryService.Iface clusterQueryHandler;

  @Inject
  private ClusterManagementService.Iface clusterManagementService;

  @Inject
  private QueryService.Iface queryHandler;

  @Inject
  private KeepAliveService.Iface keepAliveHandler;

  @Inject
  private ExecutorManager executorManager;

  @InjectOptional
  private List<ServingListener> servingListeners;

  private TThreadedSelectorServer server;

  public void serve() {
    TThreadedSelectorServer.Args serverArgs =
        createServerArgs(clusterQueryHandler, clusterManagementService, queryHandler);

    if (serverArgs == null)
      return;

    server = new ThriftServer(serverArgs, "server-selector-%d", servingListeners);

    // Make sure we at least try to clean up a little bit when the VM is shut down.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        logger.info("Shutting down server...");
        server.stop();
        executorManager.shutdownEverythingOfAllQueries();
      }
    }, "shutdown-thread"));

    if ("".equals(bind))
      logger.info("Listening for incoming requests on port {}...", port);
    else
      logger.info("Listening for incoming requests on port {} (bound to {})...", port, bind);
    server.serve();
  }

  private TThreadedSelectorServer.Args createServerArgs(ClusterQueryService.Iface clusterQueryHandler,
      ClusterManagementService.Iface clusterManagementHandler, QueryService.Iface queryHandler) {
    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    multiProcessor.registerProcessor(QueryServiceConstants.SERVICE_NAME,
        new QueryService.Processor<QueryService.Iface>(queryHandler));
    multiProcessor.registerProcessor(ClusterQueryServiceConstants.SERVICE_NAME,
        new ClusterQueryService.Processor<ClusterQueryService.Iface>(clusterQueryHandler));
    multiProcessor.registerProcessor(ClusterManagementServiceConstants.SERVICE_NAME,
        new ClusterManagementService.Processor<ClusterManagementService.Iface>(clusterManagementHandler));
    multiProcessor.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME,
        new KeepAliveService.Processor<KeepAliveService.Iface>(keepAliveHandler));

    TNonblockingServerTransport transport;
    try {
      if ("".equals(bind))
        transport = new TNonblockingServerSocket(port);
      else
        transport = new TNonblockingServerSocket(new InetSocketAddress(bind, port));
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
    serverArgs
        .executorService(executorManager.newCachedThreadPool("server-worker-%d", new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception in one of the server workers", e);
            server.stop(); // stop everything and shut down.
          }
        }));
    return serverArgs;
  }
}
