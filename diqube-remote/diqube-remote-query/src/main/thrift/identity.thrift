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
    2: bool isSuperUser,
    3: i64 validUntil
}

struct Ticket {
    // IMPORTANT: TicketUtil depends on this ordering, as it records the bytes of a serialized Ticket used to describe
    // the TicketClaim! Those bytes are then used to calculate the signature.
    1: TicketClaim claim,
    2: binary signature
}

struct OptionalString {
    1: optional string value
}

service IdentityService {
    Ticket login(1: string user, 2: string password) 
        throws (1: base.AuthenticationException authenticationException)
    
    oneway void logout(1: Ticket ticket)
    
    void changePassword(1: Ticket ticket, 2: string username, 3: string newPassword) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
        
    void changeEmail(1: Ticket ticket, 2: string username, 3: string newEmail) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
                
    void addPermission(1: Ticket ticket, 2: string username, 3: string permission, 4: OptionalString object)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
        
    void removePermission(1: Ticket ticket, 2: string username, 3: string permission, 4: OptionalString object)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
     
    map<string, list<string>> getPermissions(1: Ticket ticket, 2: string username)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)

    void createUser(1: Ticket ticket, 2: string username, 3: string email, 4: string password) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)

    void deleteUser(1: Ticket ticket, 2: string username) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
}


service IdentityCallbackService {
    oneway void ticketBecameInvalid(1: Ticket ticket)
}