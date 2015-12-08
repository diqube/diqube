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

  angular.module("diqube.login", [ "diqube.remote", "diqube.login-state" ]).controller("LoginCtrl",
      [ "remoteService", "loginStateService", "$scope", "$rootScope", "$location", 
      function(remoteService, loginStateService, $scope, $rootScope, $location) {
        var me = this;
        
        me.login = login;
        me.isLoggingIn = false;
        me.error = undefined;
        
        // ====
        
        function initialize() {
          if (loginStateService.isTicketAvailable())
            // already logged in.
            $location.path("/");
        }

        function login(user) {
          var setCookie = user.setcookie
          me.isLoggingIn = true;
          remoteService.execute("login", { username: user.name, password: user.password }, new (function() {
            this.data = function data_(dataType, data) {
              if (dataType == "ticket") {
                $scope.$apply(function() {
                  me.isLoggingIn = false;
                  loginStateService.setStoreTicketInCookie(setCookie);
                  $rootScope.$broadcast("login:succeeded");
                  loginStateService.loginSuccessful(data.ticket);
                });
              }
            }
            this.exception = function exception_(text) {
              $scope.$apply(function() {
                me.error = text;
                me.isLoggingIn = false;
                $rootScope.$broadcast("login:failed");
              });
            };
          })());
        }
        
        initialize();
      } ]);
})();