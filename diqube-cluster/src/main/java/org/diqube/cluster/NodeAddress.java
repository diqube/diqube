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
package org.diqube.cluster;

import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.util.RNodeAddressUtil;
import org.diqube.util.Pair;

/**
 * Simple representation of an address of a cluster node.
 *
 * @author Bastian Gloeckle
 */
public class NodeAddress {
  private String host;

  private Short port;

  public NodeAddress(RNodeAddress remoteAddr) {
    host = remoteAddr.getDefaultAddr().getHost();
    port = remoteAddr.getDefaultAddr().getPort();
  }

  public NodeAddress(String host, Short port) {
    this.host = host;
    this.port = port;
  }

  public NodeAddress(Pair<String, Short> addrPair) {
    this(addrPair.getLeft(), addrPair.getRight());
  }

  public String getHost() {
    return host;
  }

  public Short getPort() {
    return port;
  }

  public RNodeAddress createRemote() {
    return RNodeAddressUtil.buildDefault(host, port);
  }

  @Override
  public int hashCode() {
    return host.hashCode() ^ port.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NodeAddress))
      return false;

    NodeAddress other = (NodeAddress) obj;

    return (((host == null && other.host == null) || (host != null && host.equals(other.host))) && //
        ((port == null && other.port == null || port != null && port.equals(other.port))));
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }
}
