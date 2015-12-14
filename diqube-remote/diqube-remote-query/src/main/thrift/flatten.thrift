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

exception RFlattenPreparationException {
    1: string message
}

service FlattenPreparationService {
  void prepareForQueriesOnFlattenedTable(1: base.Ticket ticket, 2:string tableName, 3: string flattenBy) 
    throws (1: RFlattenPreparationException flattenPreparationException, 2: base.AuthenticationException authenticationException, 3:base.AuthorizationException authorizationException)

}