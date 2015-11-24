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

import java.nio.ByteBuffer;

import javax.inject.Inject;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.thrift.ClusterConsensusService;

/**
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterConsensusHandler implements ClusterConsensusService.Iface {

  @Inject
  private ClusterConsensusConnectionRegistry registry;

  @Inject
  private DiqubeCatalystConnectionFactory factory;

  @Inject
  private DiqubeCatalystServer server;

  @Override
  public void open(RUUID consensusConnectionId, RNodeAddress resultAddress) throws TException {
    DiqubeCatalystConnection newCon = factory.createDiqubeCatalystConnection();
    newCon.acceptAndRegister(RUuidUtil.toUuid(consensusConnectionId), resultAddress);
    server.newClientConnection(newCon);
  }

  @Override
  public void close(RUUID consensusConnectionId) throws TException {
    DiqubeCatalystConnection con = registry.getConnection(RUuidUtil.toUuid(consensusConnectionId));
    if (con != null)
      con.close();
  }

  @Override
  public void request(RUUID consensusConnectionId, RUUID consensusRequestId, ByteBuffer data) throws TException {
    DiqubeCatalystConnection con = registry.getConnection(RUuidUtil.toUuid(consensusConnectionId));
    if (con != null)
      con.handleRequest(RUuidUtil.toUuid(consensusRequestId), data);
  }

  @Override
  public void reply(RUUID consensusConnectionId, RUUID consensusRequestId, ByteBuffer data) throws TException {
    DiqubeCatalystConnection con = registry.getConnection(RUuidUtil.toUuid(consensusConnectionId));
    if (con != null)
      con.handleResponse(RUuidUtil.toUuid(consensusRequestId), data);
  }

  @Override
  public void replyException(RUUID consensusConnectionId, RUUID consensusRequestId, ByteBuffer data) throws TException {
    DiqubeCatalystConnection con = registry.getConnection(RUuidUtil.toUuid(consensusConnectionId));
    if (con != null)
      con.handleResponseException(RUuidUtil.toUuid(consensusRequestId), data);
  }

}
