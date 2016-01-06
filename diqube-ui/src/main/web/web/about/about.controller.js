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

  angular.module("diqube.about", [ "diqube.remote" ]).controller("AboutCtrl",
      [ "remoteService", "$scope", function(remoteService, $scope) {
        var me = this;
        me.gitcommit = "";
        me.gitcommitlong = "";
        me.buildtimestamp = "";

        // ====

        function initialize() {
          remoteService.execute("version", null, new (function() {
            this.data = function data_(dataType, data) {
              if (dataType == "version") {
                $scope.$apply(function() {
                  me.gitcommit = data.gitCommitShort;
                  me.gitcommitlong = data.gitCommitLong;
                  me.buildtimestamp = data.buildTimestamp;
                });
              }
            }
          })());
        }

        initialize();
      } ]);
})();