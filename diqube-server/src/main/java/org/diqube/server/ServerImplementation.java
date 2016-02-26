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
import org.diqube.connection.integrity.IntegrityCheckingProtocol;
import org.diqube.connection.integrity.IntegritySecretHelper;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.context.shutdown.ContextShutdownListener;
import org.diqube.context.shutdown.ShutdownUtil;
import org.diqube.listeners.ServingListener;
import org.diqube.remote.cluster.ClusterConsensusServiceConstants;
import org.diqube.remote.cluster.ClusterFlattenServiceConstants;
import org.diqube.remote.cluster.ClusterManagementServiceConstants;
import org.diqube.remote.cluster.ClusterQueryServiceConstants;
import org.diqube.remote.cluster.thrift.ClusterConsensusService;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.remote.cluster.thrift.ClusterManagementService;
import org.diqube.remote.cluster.thrift.ClusterQueryService;
import org.diqube.remote.query.ClusterInformationServiceConstants;
import org.diqube.remote.query.FlattenPreparationServiceConstants;
import org.diqube.remote.query.IdentityCallbackServiceConstants;
import org.diqube.remote.query.IdentityServiceConstants;
import org.diqube.remote.query.KeepAliveServiceConstants;
import org.diqube.remote.query.QueryServiceConstants;
import org.diqube.remote.query.thrift.ClusterInformationService;
import org.diqube.remote.query.thrift.FlattenPreparationService;
import org.diqube.remote.query.thrift.IdentityCallbackService;
import org.diqube.remote.query.thrift.IdentityService;
import org.diqube.remote.query.thrift.KeepAliveService;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.server.thrift.ThriftServer;
import org.diqube.threads.ExecutorManager;
import org.diqube.thrift.util.RememberingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;

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
  private IntegritySecretHelper integritySecretHelper;

  @Inject
  private ClusterQueryService.Iface clusterQueryHandler;

  @Inject
  private ClusterManagementService.Iface clusterManagementHandler;

  @Inject
  private ClusterInformationService.Iface clusterInformationHandler;

  @Inject
  private ClusterFlattenService.Iface clusterFlattenHandler;

  @Inject
  private QueryService.Iface queryHandler;

  @Inject
  private KeepAliveService.Iface keepAliveHandler;

  @Inject
  private FlattenPreparationService.Iface flattenPreparationHandler;

  @Inject
  private ClusterConsensusService.Iface clusterConsensusHandler;

  @Inject
  private IdentityService.Iface identityHandler;

  @Inject
  private IdentityCallbackService.Iface identityCallbackHandler;

  @Inject
  private ExecutorManager executorManager;

  @InjectOptional
  private List<ServingListener> servingListeners;

  @InjectOptional
  private List<ContextShutdownListener> gracefulShutdownListeners;

  @Inject
  private ConfigurableApplicationContext applicationContext;

  private TThreadedSelectorServer server;

  public void serve() {
    TThreadedSelectorServer.Args serverArgs = createServerArgs();

    if (serverArgs == null)
      return;

    server = new ThriftServer(serverArgs, "server-selector-%d", servingListeners);

    // Make sure we at least try to clean up a little bit when the VM is shut down.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        logger.info("Shutting down server...");
        if (gracefulShutdownListeners != null) {
          logger.debug("Executing graceful shutdown listeners...");
          new ShutdownUtil(applicationContext).callShutdownListeners();
        }
        logger.debug("Stopping all local beans...");
        applicationContext.close();
        logger.debug("Stopping thrift server...");
        server.stop();
        logger.debug("Shutting down everything of remaining queries...");
        executorManager.shutdownEverythingOfAllQueries();
        logger.info("Shutdown complete.");
      }
    }, "shutdown-thread"));

    if ("".equals(bind))
      logger.info("Listening for incoming requests on port {}...", port);
    else
      logger.info("Listening for incoming requests on port {} (bound to {})...", port, bind);
    server.serve();
  }

  private TThreadedSelectorServer.Args createServerArgs() {
    TMultiplexedProcessor multiProcessor = new TMultiplexedProcessor();

    // not-integrity-checked services: communication from "outside" of diqube-servers
    multiProcessor.registerProcessor(QueryServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new QueryService.Processor<QueryService.Iface>(queryHandler)));
    multiProcessor.registerProcessor(KeepAliveServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new KeepAliveService.Processor<KeepAliveService.Iface>(keepAliveHandler)));
    multiProcessor.registerProcessor(FlattenPreparationServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new FlattenPreparationService.Processor<FlattenPreparationService.Iface>(flattenPreparationHandler)));
    multiProcessor.registerProcessor(ClusterInformationServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new ClusterInformationService.Processor<ClusterInformationService.Iface>(clusterInformationHandler)));
    multiProcessor.registerProcessor(IdentityServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new IdentityService.Processor<IdentityService.Iface>(identityHandler)));
    multiProcessor.registerProcessor(IdentityCallbackServiceConstants.SERVICE_NAME,
        new IntegrityCheckingProtocol.IntegrityCheckDisablingProcessor(
            new IdentityCallbackService.Processor<IdentityCallbackService.Iface>(identityCallbackHandler)));

    // integrity-checked services: Communication between diqube-servers
    multiProcessor.registerProcessor(ClusterQueryServiceConstants.SERVICE_NAME,
        new ClusterQueryService.Processor<ClusterQueryService.Iface>(clusterQueryHandler));
    multiProcessor.registerProcessor(ClusterManagementServiceConstants.SERVICE_NAME,
        new ClusterManagementService.Processor<ClusterManagementService.Iface>(clusterManagementHandler));
    multiProcessor.registerProcessor(ClusterFlattenServiceConstants.SERVICE_NAME,
        new ClusterFlattenService.Processor<ClusterFlattenService.Iface>(clusterFlattenHandler));
    multiProcessor.registerProcessor(ClusterConsensusServiceConstants.SERVICE_NAME,
        new ClusterConsensusService.Processor<ClusterConsensusService.Iface>(clusterConsensusHandler));

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
    serverArgs.transportFactory(new RememberingTransport.Factory(new TFramedTransport.Factory()));
    serverArgs.protocolFactory(new IntegrityCheckingProtocol.Factory(new TCompactProtocol.Factory(),
        integritySecretHelper.provideMessageIntegritySecrets()));
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
