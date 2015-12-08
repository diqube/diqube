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
      [ "$log", "$cookies", "$location", function loginStateProvider($log, $cookies, $location) {
        var me = this;
        
        me.getTicket = getTicket;
        me.loginSuccessful = loginSuccessful;
        me.logoutSuccessful = logoutSuccessful;
        me.setStoreTicketInCookie = setStoreTicketInCookie;
        me.isTicketAvailable = isTicketAvailable;
        me.loginAndReturnHere = loginAndReturnHere; 

        // ==

        me.ticket = undefined;
        me.isSecure = undefined;
        me.initialize = initialize;
        me.returnPathAfterLogin = undefined;
        me.storeTicketInCookie = false;

        function loginSuccessful(ticket) {
          me.ticket = ticket;
          
          if (me.storeTicketInCookie) {
            if (me.isSecure)
              $cookies.put("DiqubeTicket", me.ticket, { secure: true });
            else
              $cookies.put("DiqubeTicket", me.ticket);
          } else
            $cookies.remove("DiqubeTicket");
          
          if (me.returnPathAfterLogin) {
            var p = me.returnPathAfterLogin;
            me.returnPathAfterLogin = undefined;
            $location.path(p);
          } else 
            $location.path("/");
        }
        
        function logoutSuccessful() {
          me.ticket = undefined;
          $cookies.remove("DiqubeTicket");
          $location.path("/");
        }
        
        function getTicket() {
          return me.ticket;
        }
        
        function setStoreTicketInCookie(storeTicketInCookie) {
          me.storeTicketInCookie = storeTicketInCookie;
        }
        
        function initialize() {
          if ($cookies.get("DiqubeTicket"))
            me.ticket = $cookies.get("DiqubeTicket");
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
          if (cookie)
            me.ticket = cookie;
        }
        
      } ]).run([ "loginStateService", "$location", function loginStateRun(loginStateService, $location) {
        loginStateService.initialize($location);
      } ]);
})();