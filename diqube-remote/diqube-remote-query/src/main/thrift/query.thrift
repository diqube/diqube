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

namespace java org.diqube.remote.query.thrift

include "${diqube.thrift.dependencies}/base.thrift"


struct RResultTable {
  1: list<string> columnNames,
  2: optional list<list<base.RValue>> rows
}

exception RQueryException {
  1: string message,
}

service QueryService {
  void asyncExecuteQuery(1: base.RUUID queryRUuid, 
                         2: string diql, 
                         3: bool sendPartialUpdates, 
                         4: base.RNodeAddress resultAddress) throws (1: RQueryException queryException)
  void cancelQueryExecution(1: base.RUUID queryRUuid)
}

struct RQueryStatisticsDetails {
  1: string node,
  2: i64 startedUntilDoneMs,
  3: i32 numberOfThreads,
  4: i32 numberOfTemporaryColumnShardsCreated,
  5: i32 numberOfTemporaryColumnShardsFromCache,
  6: map<string, i64> stepsActiveMs,
  7: map<string, i32> numberOfPageAccesses,
  8: map<string, i32> numberOfTemporaryPageAccesses,
  9: i32 numberOfPagesInTable,
  10: i32 numberOfTemporaryPages,
  11: map<string, i32> numberOfTemporaryVersionsPerColName
}  


struct RQueryStatistics {
  1: RQueryStatisticsDetails master,
  2: list<RQueryStatisticsDetails> remotes
}

service QueryResultService {
  oneway void partialUpdate(1:base.RUUID queryRUuid, 2:RResultTable partialResult, 3:i16 percentComplete),
  oneway void queryResults(1:base.RUUID queryRUuid, 2:RResultTable finalResult),
  oneway void queryException(1:base.RUUID queryRUuid, 2:RQueryException exceptionThrown),
  oneway void queryStatistics(1:base.RUUID queryRuuid, 2: RQueryStatistics stats)
}