//
// diqube: Distributed Query Base.
//
// Copyright (C) 2015 Bastian Gloeckle
//
// This file is part of diqube.
//
// diqube is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

namespace java org.diqube.remote.cluster.thrift

include "${diqube.thrift.dependencies}/base.thrift"

exception RConnectionUnknownException {
  1: string msg
}

service ClusterConsensusService {
  base.RUUID open(1: base.RUUID otherConsensusConnectionEndpointId, 2:base.RNodeAddress resultAddress)
  
  void close(1: base.RUUID consensusConnectionEndpointId) throws (1: RConnectionUnknownException connectionUnknownException)

  void request(1: base.RUUID consensusConnectionEndpointId, 2: base.RUUID consensusRequestId, 3: binary data) throws (1: RConnectionUnknownException connectionUnknownException)
  
  void reply(1: base.RUUID consensusConnectionEndpointId, 2: base.RUUID consensusRequestId, 3: binary data) throws (1: RConnectionUnknownException connectionUnknownException)
  
  void replyException(1: base.RUUID consensusConnectionEndpointId, 2: base.RUUID consensusRequestId, 3: binary data) throws (1: RConnectionUnknownException connectionUnknownException)
}