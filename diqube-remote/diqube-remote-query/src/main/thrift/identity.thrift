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

struct TicketClaim {
    1: string username,
    2: i64 validUntil
}

struct Ticket {
    // IMPORTANT: TicketUtil depends on this ordering, as it records the bytes of a serialized Ticket used to describe
    // the TicketClaim! Those bytes are then used to calculate the signature.
    1: TicketClaim claim,
    2: binary signature
}

exception AuthenticationException {
    1: string msg 
}

service IdentityService {
    Ticket login(1: string user, 2: string password) throws (1: AuthenticationException authenticationException)
    
    oneway void logout(1: Ticket ticket)
}


service IdentityCallbackService {
    oneway void ticketBecameInvalid(1: Ticket ticket)
}