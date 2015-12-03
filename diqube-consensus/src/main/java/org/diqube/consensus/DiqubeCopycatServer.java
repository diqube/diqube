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
package org.diqube.consensus;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.consensus.internal.DiqubeCatalystClient;
import org.diqube.consensus.internal.DiqubeCatalystSerializer;
import org.diqube.consensus.internal.DiqubeCatalystTransport;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.listeners.DiqubeConsensusListener;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.RaftServer.State;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

/**
 * Instance of the copycat server which is part of the copycat cluster and provides a consensus cluster for us.
 * 
 * <p>
 * Interact with that cluster through the {@link RaftClient} returned by {@link DiqubeCatalystClient}.
 * 
 * <p>
 * This will automatically join the copycat cluster which is defined by the node addresses returned by
 * {@link ConsensusClusterNodeAddressProvider}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCopycatServer implements ClusterManagerListener {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeCopycatServer.class);

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  @Inject
  private ConsensusClusterNodeAddressProvider consensusClusterNodeAddressProvider;

  @Inject
  private DiqubeCatalystTransport transport;

  @Inject
  private DiqubeCatalystSerializer serializer;

  @InjectOptional
  private List<DiqubeConsensusListener> listeners;

  @Config(ConfigKey.CONSENSUS_DATA_DIR)
  private String consensusDataDir;

  @Config(ConfigKey.DATA_DIR)
  private String dataDir;

  @Config(ConfigKey.KEEP_ALIVE_MS)
  private int keepAliveMs;

  @Inject
  private DiqubeConsensusStateMachineManager stateMachineManager;

  @Inject
  private ApplicationContext beanContext;

  // extracted for tests
  private ConsensusStorageProvider consensusStorageProvider = null;

  private CopycatServer copycatServer;

  private long sessionTimeoutMs;

  private long electionTimeoutMs;

  private Address lastKnownCopycatLeaderAddress = null;
  private Instant lastKnownCopycatLeaderTimestamp = null;

  @PostConstruct
  public void initialize() {
    sessionTimeoutMs = 30 * keepAliveMs; // same approx distribution as the defaults of copycat.
    electionTimeoutMs = 6 * keepAliveMs;
  }

  @Override
  public void clusterInitialized() {
    Address ourAddr = toCopycatAddress(ourNodeAddressProvider.getOurNodeAddress());
    List<Address> members = consensusClusterNodeAddressProvider.getClusterNodeAddressesForConsensus().stream()
        .map(addr -> toCopycatAddress(addr)).collect(Collectors.toList());

    File consensusDataDirFile = new File(consensusDataDir);
    if (!consensusDataDirFile.isAbsolute())
      consensusDataDirFile = new File(new File(dataDir), consensusDataDir);

    if (!consensusDataDirFile.exists())
      if (!consensusDataDirFile.mkdirs())
        throw new RuntimeException("Could not create consenusDataDir at " + consensusDataDirFile.getAbsolutePath()
            + ". Restart diqube-server!");

    logger.info("Starting up consensus node with local data dir at '{}'.", consensusDataDirFile.getAbsolutePath());
    if (consensusStorageProvider == null)
      consensusStorageProvider = new ConsensusStorageProvider(consensusDataDirFile);
    Storage storage = consensusStorageProvider.createStorage();

    copycatServer = CopycatServer.builder(ourAddr, members). //
        withTransport(transport). //
        withStorage(storage). //
        withSerializer(serializer). //
        withSessionTimeout(Duration.ofMillis(sessionTimeoutMs)). //
        withElectionTimeout(Duration.ofMillis(electionTimeoutMs)). //
        withHeartbeatInterval(Duration.ofMillis(keepAliveMs)). //
        withStateMachine(new DiqubeStateMachine()).build();

    copycatServer.open().handle((result, error) -> {
      if (error != null)
        throw new RuntimeException("Could not start Consensus node. Restart diqube-server!", error);

      logger.info("Consensus node started successfully.");

      copycatServer.onLeaderElection(leaderAddr -> {
        if (leaderAddr != null) {
          lastKnownCopycatLeaderAddress = leaderAddr;
          lastKnownCopycatLeaderTimestamp = Instant.now();
          logger.info("New consensus leader address: {}", lastKnownCopycatLeaderAddress);
        }
      });
      lastKnownCopycatLeaderAddress = copycatServer.leader();
      lastKnownCopycatLeaderTimestamp = Instant.now();
      logger.info("New consensus leader address: {}", lastKnownCopycatLeaderAddress);

      if (listeners != null)
        listeners.forEach(l -> l.consensusInitialized());

      return null;
    });
  }

  @PreDestroy
  public void stop() {
    if (copycatServer != null) {
      logger.debug("Closing consensus server...");
      Thread closeThread = new Thread(() -> copycatServer.close().join(), "consensus-shutdown");
      closeThread.start();
      // copycat will retry after election timeout, give it some possibility to retry.
      long copycatShutdownTimeoutMs = electionTimeoutMs * 2 + 2;
      try {
        closeThread.join(copycatShutdownTimeoutMs);
      } catch (InterruptedException e) {
        // swallow.
      }
      if (closeThread.isAlive())
        logger.warn(
            "Consensus server failed to stop withtin the timeout ({} ms). Maybe the consensus leader node cannot be "
                + "reached? The last known leader node is {} ({}). Will continue shutdown anyway. "
                + "Note that this server might not have been deregistered from the consensus cluster completely.",
            copycatShutdownTimeoutMs, lastKnownCopycatLeaderAddress, lastKnownCopycatLeaderTimestamp);
      else
        logger.debug("Consensus server closed.");
      copycatServer = null;
    }
  }

  /**
   * @return true if this node is the leader of the consensus cluster.
   */
  public boolean isLeader() {
    return copycatServer != null && copycatServer.state().equals(State.LEADER);
  }

  /* package */ Collection<Address> getClusterMembers() {
    if (copycatServer == null)
      return null;
    return copycatServer.members();
  }

  private Address toCopycatAddress(NodeAddress addr) {
    return new Address(addr.getHost(), addr.getPort());
  }

  private class DiqubeStateMachine extends StateMachine {
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected void configure(StateMachineExecutor executor) {
      for (Class<? extends Operation<?>> operationClass : stateMachineManager.getAllOperationClasses()) {
        Pair<Class<?>, Method> p = stateMachineManager.getImplementation(operationClass);
        Object targetBean = beanContext.getBean(p.getLeft());
        Method targetMethod = p.getRight();
        if (targetMethod.getReturnType().equals(Void.TYPE)) {
          executor.register((Class) operationClass, ((Consumer) new Consumer<Object>() {
            @Override
            public void accept(Object t) {
              try {
                targetMethod.invoke(targetBean, t);
              } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new DiqubeStateExceptionExecutionException("Could not invoke state machine method", e);
              }
            }
          }));
        } else {
          executor.register((Class) operationClass, ((Function) new Function<Object, Object>() {
            @Override
            public Object apply(Object t) {
              try {
                return targetMethod.invoke(targetBean, t);
              } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new DiqubeStateExceptionExecutionException("Could not invoke state machine method", e);
              }
            }
          }));
        }
      }
    }
  }

  public static class DiqubeStateExceptionExecutionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DiqubeStateExceptionExecutionException(String msg) {
      super(msg);
    }

    public DiqubeStateExceptionExecutionException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /* package */ static class ConsensusStorageProvider {
    private File consensusDataDirFile;

    ConsensusStorageProvider(File consensusDataDirFile) {
      this.consensusDataDirFile = consensusDataDirFile;
    }

    public Storage createStorage() {
      return Storage.builder().withStorageLevel(StorageLevel.DISK).withDirectory(consensusDataDirFile).build();
    }
  }

  // for tests
  /* package */ void setConsensusStorageProvider(ConsensusStorageProvider consensusStorageProvider) {
    this.consensusStorageProvider = consensusStorageProvider;
  }

  /* package */ long getElectionTimeoutMs() {
    return electionTimeoutMs;
  }
}
