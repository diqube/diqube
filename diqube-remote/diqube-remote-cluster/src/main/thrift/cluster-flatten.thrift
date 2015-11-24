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

exception RFlattenException {
  1: string message
}

exception RRetryLaterException {
  1: string message
}

struct ROptionalUuid {
  1: optional base.RUUID uuid
}

service ClusterFlattenService {
  void flattenAllLocalShards(1: base.RUUID flattenRequestId, 2: string tableName, 3: string flattenBy, 
    4: list<base.RNodeAddress> otherFlatteners, 5: base.RNodeAddress resultAddress) throws (1: RFlattenException flattenException),
  
  void shardsFlattened(1: base.RUUID flattenRequestId, 
    2: map<i64, i64> origShardFirstRowIdToFlattenedNumberOfRowsDelta, 3: base.RNodeAddress flattener) throws (1: RRetryLaterException retryLaterException),
    
  ROptionalUuid getLatestValidFlattening(1: string tableName, 2: string flattenBy) throws (1: RFlattenException flattenException)
    
  oneway void flattenDone(1: base.RUUID flattenRequestId, 2: base.RUUID flattenedTableId, 
    3: base.RNodeAddress flattener)
    
  oneway void flattenFailed(1: base.RUUID flattenRequestId, 2: RFlattenException flattenException)
}