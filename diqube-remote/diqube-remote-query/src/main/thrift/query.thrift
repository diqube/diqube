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
  2: list<list<base.RValue>> rows
  // TODO type needed here?
}

exception RQueryException {
  1: string message,
}

service QueryService {
  RResultTable syncExecuteQuery(1: string diql) throws (1: RQueryException queryException),
  void asyncExecuteQuery(1: base.RUUID queryRUuid, 
                         2: string diql, 
                         3: bool sendPartialUpdates, 
                         4: base.RNodeAddress resultAddress) throws (1: RQueryException queryException)
}

service QueryResultService {
  oneway void partialUpdate(1:base.RUUID queryRUuid, 2:RResultTable partialResult, 3:i16 percentComplete),
  oneway void queryResults(1:base.RUUID queryRUuid, 2:RResultTable finalResult),
  oneway void queryException(1:base.RUUID queryRUuid, 2:RQueryException exceptionThrown)
}