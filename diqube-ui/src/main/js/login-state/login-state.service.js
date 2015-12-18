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
'use strict';

(function() {
  angular.module("diqube.login-state").service("loginStateService",
      [ "$log", "$cookies", "$location", "$window", function loginStateProvider($log, $cookies, $location, $window) {
        var me = this;
        
        me.ticket = undefined; // ticket as to be used to make remote calls.
        me.username = undefined; // username inside the ticket.
        me.loginSuccessful = loginSuccessful;
        me.logoutSuccessful = logoutSuccessful;
        me.setStoreTicketInCookie = setStoreTicketInCookie;
        me.isTicketAvailable = isTicketAvailable;
        me.loginAndReturnHere = loginAndReturnHere; 

        // ==

        me.isSecure = undefined;
        me.returnPathAfterLogin = undefined;
        me.storeTicketInCookie = false;
        me.initialize = initialize;

        // ticket: the serialized ticket. username: the username of the ticket.
        function loginSuccessful(ticket, username) {
          me.ticket = ticket;
          me.username = username;
          
          if (me.storeTicketInCookie) {
            if (me.isSecure) {
              $cookies.put("DiqubeTicket", me.ticket, { secure: true });
              $cookies.put("DiqubeUser", me.username, { secure: true });
            } else {
              $cookies.put("DiqubeTicket", me.ticket);
              $cookies.put("DiqubeUser", me.username);
            }
          } else {
            $cookies.remove("DiqubeTicket");
            $cookies.remove("DiqubeUser");
          }
          
          if (me.returnPathAfterLogin) {
            var p = me.returnPathAfterLogin;
            me.returnPathAfterLogin = undefined;
            $location.path(p);
            me.returnPathAfterLogin = undefined;
          } else 
            $location.path("/");
        }
        
        function logoutSuccessful() {
          me.ticket = undefined;
          me.username = undefined;
          me.returnPathAfterLogin = undefined;
          
          $cookies.remove("DiqubeTicket");
          $cookies.remove("DiqubeUser");
          // use JavaScript global "location" to reload the current page.
          if (location)
            location.reload();
        }
        
        function setStoreTicketInCookie(storeTicketInCookie) {
          me.storeTicketInCookie = storeTicketInCookie;
        }
        
        function isTicketAvailable() {
          return !!me.ticket;
        }
        
        function loginAndReturnHere() {
          me.returnPathAfterLogin = $location.path();
          $location.path("/login");
        }
        
        function initialize($location) {
          me.isSecure = $location.protocol().toLowerCase() === "https";
          var cookie = $cookies.get("DiqubeTicket");
          if (cookie) {
            me.ticket = cookie;
            me.username = $cookies.get("DiqubeUser");
          }
        }
        
      } ]).run([ "loginStateService", "$location", function loginStateRun(loginStateService, $location) {
        loginStateService.initialize($location);
      } ]);
})();