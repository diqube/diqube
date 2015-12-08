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
(function() {
  "use strict";

  angular.module("diqube.logout", [ "diqube.remote", "diqube.login-state" ]).controller("LogoutCtrl",
      [ "remoteService", "loginStateService", "$scope", "$rootScope", function(remoteService, loginStateService, $scope, $rootScope) {
        var me = this;
        
        me.logout = logout;
        me.isLogoutAvailable = isLogoutAvailable;
        
        // ====
        
        me.logoutSuccessful = logoutSuccessful; 

        function logout() {
          remoteService.execute("logout", null, new (function() {
            this.done = function done_() {
              $scope.$apply(function() {
                me.logoutSuccessful();
              });
            }
            this.data = function data_(dataType, data) {
            }
            this.exception = function exception_(text) {
              $scope.$apply(function() {
                // we should not receive an exception when logging out, but if we do, lets assume the logout itself
                // worked anyway.
                me.logoutSuccessful();
              });
            };
          })());
        }
        
        function isLogoutAvailable() {
          return loginStateService.isTicketAvailable();
        }
        
        function logoutSuccessful() {
          $rootScope.$broadcast("logout:successful");
          loginStateService.logoutSuccessful();
        }
      } ]);
})();