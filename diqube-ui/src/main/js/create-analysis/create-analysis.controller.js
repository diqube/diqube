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
  

  angular.module("diqube.create-analysis", [ "diqube.remote", "diqube.analysis" ]).controller(
      "CreateAnalysisCtrl", [ "remoteService",  "analysisService", "$scope", "$rootScope", "$log", 
      function(remoteService, analysisService, $scope, $rootScope, $log) {
        var me = this;
        
        me.createAnalysis = createAnalysis;
        me.getValidTables = getValidTables;
        me.error = undefined;
        
        // ====
        
        function createAnalysis(analysis) {
          if (analysis === undefined) {
            me.error = "Name and Table are required.";
            return;
          }
          if (!analysis.hasOwnProperty("name") || !analysis.name) {
            me.error = "Name is required.";
            return;
          }
          if (!analysis.hasOwnProperty("table") || !analysis.table) {
            me.error = "Table is required.";
            return;
          }
          me.error = undefined;
          $log.info("Creating analysis ", analysis.name, " on table ", analysis.table);
          remoteService.execute($scope, "createAnalysis", { table: analysis.table, name: analysis.name }, 
              new (function() {
                this.data = function data_(dataType, data) {
                  if (dataType === "analysis") {
                    analysisService.setLoadedAnalysis(data.analysis);
                  }
                };
                this.exception = function exception_(text) {
                  me.error = text;
                };
                this.done = function done_() {
                  $log.info("Created new analysis.");
                  $rootScope.$broadcast("analysis:created");
                }
          })());
        }
        
        function getValidTables(userInput) {
          return new Promise(function(resolve, reject) {
            resolve([ { name: userInput } ]);
          });
        }
      } ]);
})();