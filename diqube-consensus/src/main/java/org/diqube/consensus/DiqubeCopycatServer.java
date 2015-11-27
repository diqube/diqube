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
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.connection.NodeAddress;
import org.diqube.connection.OurNodeAddressProvider;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.InjectOptional;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.listeners.DiqubeConsensusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.RaftClient;
import io.atomix.copycat.server.CopycatServer;
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
 * {@link ClusterNodeAddressProvider}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCopycatServer implements ClusterManagerListener {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeCopycatServer.class);

  @Inject
  private OurNodeAddressProvider ourNodeAddressProvider;

  @Inject
  private ClusterNodeAddressProvider clusterNodeAddressProvider;

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

  private CopycatServer copycatServer;

  @Override
  public void clusterInitialized() {
    Address ourAddr = toCopycatAddress(ourNodeAddressProvider.getOurNodeAddress());
    List<Address> members = clusterNodeAddressProvider.getClusterNodeAddresses().stream()
        .map(addr -> toCopycatAddress(addr)).collect(Collectors.toList());

    File consensusDataDirFile = new File(consensusDataDir);
    if (!consensusDataDirFile.isAbsolute())
      consensusDataDirFile = new File(new File(dataDir), consensusDataDir);

    if (!consensusDataDirFile.exists())
      if (!consensusDataDirFile.mkdirs())
        throw new RuntimeException("Could not create consenusDataDir at " + consensusDataDirFile.getAbsolutePath()
            + ". Restart diqube-server!");

    logger.info("Starting up consensus node with local data dir at '{}'.", consensusDataDirFile.getAbsolutePath());
    Storage storage = Storage.builder().withStorageLevel(StorageLevel.DISK).withDirectory(consensusDataDirFile).build();

    copycatServer = CopycatServer.builder(ourAddr, members). //
        withTransport(transport). //
        withStorage(storage). //
        withSerializer(serializer). //
        withSessionTimeout(Duration.ofMillis(30 * keepAliveMs)). // same approx distribution as the defaults of copycat.
        withElectionTimeout(Duration.ofMillis(6 * keepAliveMs)). //
        withHeartbeatInterval(Duration.ofMillis(keepAliveMs)). //
        withStateMachine(new StateMachine() {
          @Override
          protected void configure(StateMachineExecutor executor) {
            // executor.register(type, callback)
          }
        }).build();

    copycatServer.open().handle((result, error) -> {
      if (error != null)
        throw new RuntimeException("Could not start Consensus node. Restart diqube-server!", error);

      logger.info("Consensus node started successfully.");

      if (listeners != null)
        listeners.forEach(l -> l.consensusInitialized());

      return null;
    });
  }

  @PreDestroy
  public void stop() {
    if (copycatServer != null) {
      copycatServer.close().join();
      copycatServer = null;
    }
  }

  private Address toCopycatAddress(NodeAddress addr) {
    return new Address(addr.getHost(), addr.getPort());
  }

}
