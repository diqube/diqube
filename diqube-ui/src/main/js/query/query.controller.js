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

  angular.module("diqube.query", [ "angular-toArrayFilter", "diqube.remote" ]).controller("QueryCtrl",
      [ "remoteService", "$scope", function(remoteService, $scope) {
        var me = this;
        me.diql = "";
        me.result = null;
        me.exception = null;
        me.stats = null;
        me.displayResultsOrStats = undefined;
        me.displayResults = displayResults;
        me.displayStats = displayStats;
        me.isExecuting = false;

        me.execute = execute;
        me.cancel = cancel;

        // ====

        me.currentRequestId = null;

        function execute() {
          if (me.isExecuting)
            return;

          me.data = null;
          me.exception = null;
          me.stats = null;
          me.displayResultsOrStats = undefined;

          var fn = this;
          fn.lastPercentComplete = 0;

          me.isExecuting = true;
          me.currentRequestId = remoteService.execute($scope, "query", {
            diql : me.diql
          }, new (function() {
            this.data = function data_(dataType, data) {
              if (dataType == "table" && me.exception === null) {
                if (data.percentComplete >= fn.lastPercentComplete) {
                  me.result = data;
                  me.displayResultsOrStats = "results";

                  fn.lastPercentComplete = me.result.percentComplete;
                }
              } else if (dataType == "stats" && me.exception === null) {
                me.stats = data;
              }
              return false; // not yet done.
            };
            this.exception = function exception_(text) {
              me.exception = text;
              me.result = null;
              me.stats = null;

              me.isExecuting = false;
            }
            this.done = function done_() {
              me.isExecuting = false;
            }
          })());
        }

        function cancel() {
          if (!me.isExecuting)
            return;
          remoteService.cancel(me.currentRequestId);
          me.currentRequestId = null;
          me.data = null;
          me.exception = null;
          me.stats = null;
          me.displayResultsOrStats = undefined;

          me.isExecuting = false;
        }

        function displayResults() {
          me.displayResultsOrStats = "results";
        }

        function displayStats() {
          me.displayResultsOrStats = "stats";
        }
      } ]);
})();