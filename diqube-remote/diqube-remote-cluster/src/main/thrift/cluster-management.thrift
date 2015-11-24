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


service ClusterManagementService {
  // a new node says hello to all cluster nodes, returns the current version number of the list table it serves.
  i64 hello(1: base.RNodeAddress newNode),
  
  // After a new node has said hello, it will fetch the current active nodes in the whole cluster and the tablenames
  // they are serving. Mapping from node address to a single-entry map containing the version number of the layout of 
  // the node and the tableNames it currently serves shards of.
  map<base.RNodeAddress, map<i64, list<string>>> clusterLayout(),
  
  // return a single-entry map containing the current version of what tables the node serves parts of.
  map<i64, list<string>> fetchCurrentTablesServed(),
  
  oneway void newNodeData(1: base.RNodeAddress nodeAddr, 2:i64 version, 3:list<string> tables),
  
  oneway void nodeDied(1: base.RNodeAddress nodeAddr)
}
