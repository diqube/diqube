/*
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
"use strict";

var MockedLoginStateService = (function() {
  function MockedLoginStateService() {
    var me = this;
    
    me.getTicket = getTicket;
    me.loginSuccessful = loginSuccessful;
    me.logoutSuccessful = logoutSuccessful;
    me.setStoreTicketInCookie = setStoreTicketInCookie;
    me.isTicketAvailable = isTicketAvailable;
    me.loginAndReturnHere = loginAndReturnHere; 
    
    // ===
    
    me.ticket = "testticket";
    
    function getTicket() {
      return me.ticket;
    }
    
    function loginSuccessful(ticket) {
      me.ticket = ticket;
    }
    
    function logoutSuccessful() {
      me.ticket = undefined;
    }
    
    function setStoreTicketInCookie() {
    }
   
    function isTicketAvailable() {
      return !!me.ticket;
    }
    
    function loginAndReturnHere() {
      me.ticket="helloworld";
    }
  }
  
  return MockedLoginStateService;
})();