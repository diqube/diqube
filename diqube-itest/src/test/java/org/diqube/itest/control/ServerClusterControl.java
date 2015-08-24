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
package org.diqube.itest.control;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.diqube.itest.control.ServerControl.ServerAddr;
import org.diqube.itest.control.ServerControl.ServerAddressProvider;
import org.diqube.itest.control.ServerControl.ServerClusterNodesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controls a cluster of diqube-servers
 *
 * @author Bastian Gloeckle
 */
public class ServerClusterControl implements ServerAddressProvider, ServerClusterNodesProvider {
  private static final Logger logger = LoggerFactory.getLogger(ServerClusterControl.class);

  private List<ServerControl> servers;
  private List<ServerAddr> reservedAddrs = new ArrayList<>();

  private short nextPort = 5101; // TODO find port dynamically.

  public ServerClusterControl() {
  }

  public void setServers(List<ServerControl> servers) {
    this.servers = servers;
  }

  @Override
  public ServerAddr reserveAddress() {
    ServerAddr newAddr = new ServerAddr("127.0.0.1", nextPort++);
    reservedAddrs.add(newAddr);
    return newAddr;
  }

  @Override
  public String getClusterNodeConfigurationString(ServerAddr serverAddr) {
    List<String> serverAddrsString = reservedAddrs.stream().filter(addr -> !serverAddr.equals(addr))
        .map(addr -> addr.getHost() + ":" + addr.getPort()).collect(Collectors.toList());
    return String.join(",", serverAddrsString);
  }

  public void start() {
    logger.info("Starting cluster of {} cluster nodes.", servers.size());
    for (ServerControl server : servers) {
      server.start();
    }

    logger.info("All {} cluster nodes running.", servers.size());
  }

  public void stop() {
    logger.info("Stopping all cluster nodes: {}", reservedAddrs);
    for (ServerControl server : servers)
      server.stop();
    logger.info("All {} cluster nodes stopped.", servers.size());
  }

}
