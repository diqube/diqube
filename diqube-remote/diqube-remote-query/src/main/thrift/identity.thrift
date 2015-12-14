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

struct OptionalString {
    1: optional string value
}

struct TicketInfo {
    1: base.RUUID ticketId,
    2: i64 validUntil
}

service IdentityService {
    base.Ticket login(1: string user, 2: string password) 
        throws (1: base.AuthenticationException authenticationException)
    
    oneway void logout(1: base.Ticket ticket)
    
    void changePassword(1: base.Ticket ticket, 2: string username, 3: string newPassword) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
        
    void changeEmail(1: base.Ticket ticket, 2: string username, 3: string newEmail) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
                
    void addPermission(1: base.Ticket ticket, 2: string username, 3: string permission, 4: OptionalString object)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
        
    void removePermission(1: base.Ticket ticket, 2: string username, 3: string permission, 4: OptionalString object)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
     
    map<string, list<string>> getPermissions(1: base.Ticket ticket, 2: string username)
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)

    void createUser(1: base.Ticket ticket, 2: string username, 3: string email, 4: string password) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)

    void deleteUser(1: base.Ticket ticket, 2: string username) 
        throws (1: base.AuthenticationException authenticationException, 2: base.AuthorizationException authorizationException)
        
    string getEmail(1: base.Ticket ticket, 2: string username)
        
    // Register a node that implements IdentityCallbackService to be informed about anything interesting.
    // Node should unregsiter itself.
    // Node will be automatically deregistered after 1h, re-register accordingly.
    
    // Note that the callback might not be actually called in case no cluster node can communicate with the callback.
    // The callback there _has to_ call #getInvalidTickets() regularily to fill up its internal list of invalid tickets.
    void registerCallback(1: base.RNodeAddress nodeAddress)
    
    void unregisterCallback(1: base.RNodeAddress nodeAddress)
    
    // Retruns the Tickets that are marked invalid. Note that tickets that are invalid because of values in their claim 
    // ("validUntil"), might not be returned by this method, although these Tickets might have been used in calls to 
    // #logout.
    // If a client relies on knowing the invalid tickets and cannot call this method (e.g. because of a network 
    // partition) it should not accept ANY ticket anymore, since it does not know the current list of invalid tickets.
    // This returns only "TicketInfo"s instead of full tickets, so possible attackers cannot simply register and learn
    // about usernames. Remember: This service is not integrity validated, so everybody can talk to this service, not 
    // only diqube-servers.
    list<TicketInfo> getInvalidTicketInfos()
}


service IdentityCallbackService {
    // A specific ticket became invalid, although the "validUntil" might still be in the future. The ticket must not be
    // accepted any more.
    // Note that this method might be called multiple times with the same ticket!
    oneway void ticketBecameInvalid(1: TicketInfo ticketInfo)
}