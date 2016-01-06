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
      "CreateAnalysisCtrl", [ "remoteService",  "analysisService", "$scope", "$rootScope", "$log", "$location", "loginStateService",
      function(remoteService, analysisService, $scope, $rootScope, $log, $location, loginStateService) {
        var me = this;
        
        me.createAnalysis = createAnalysis;
        me.getValidTables = getValidTables;
        me.error = undefined;
        me.isCreating = false;
        
        me.nameValid = undefined;
        me.tableValid = undefined;
        
        me.validateName = validateName;
        me.validateTable = validateTable;
        
        // ====
        
        me.allTables = undefined;
        
        me.allTablePromise = new Promise(function(resolve, reject) {
          if (me.allTables !== undefined) {
            resolve(me.allTables);
          } else {
            remoteService.execute("listAllTables", null, new (function() {
              this.data = function data_(dataType, data) {
                if (dataType === "tableNameList") {
                  $scope.$apply(function() {
                    me.allTables = [];
                    for (var idx in data.tableNames)
                      me.allTables.push({ name : data.tableNames[idx] });
                  });
                  resolve(me.allTables);
                }
              }
              this.exception = function exception_(text) {
                reject(text);
              }
            })());
          }
        });
        
        function initialize() {
          if (!loginStateService.isTicketAvailable()) {
            loginStateService.loginAndReturnHere();
            return;
          }
        }
        
        function createAnalysis(analysis) {
          if (analysis === undefined) {
            me.error = "Name and Table are required.";
            me.nameValid = false;
            me.tableValid = false;
            return;
          }
          me.error = validateName(analysis.name);
          if (!me.nameValid)
            return;
          
          me.error = validateTable(analysis.table);
          if (!me.tableValid)
            return;
          
          me.isCreating = true;
          me.error = undefined;
          $log.info("Creating analysis ", analysis.name, " on table ", analysis.table);
          remoteService.execute("createAnalysis", { table: analysis.table, name: analysis.name }, 
              new (function() {
                this.data = function data_(dataType, data) {
                  if (dataType === "analysis") {
                    $scope.$apply(function() {
                      analysisService.setLoadedAnalysis(data.analysis);
                    });
                  }
                };
                this.exception = function exception_(text) {
                  $scope.$apply(function() {
                    me.error = text;
                    me.isCreating = false;
                  });
                };
                this.done = function done_() {
                  $log.info("Created new analysis.");
                  $rootScope.$broadcast("analysis:created", analysisService.loadedAnalysis);
                  $scope.$apply(function() {
                    me.isCreating = false;
                  });
                  $location.hash("");
                  $location.path("analysis/" + analysisService.loadedAnalysis.id);
                }
          })());
        }
        
        function getValidTables(userInput) {
          return me.allTablePromise.then(function(tables) {
            return tables.filter(function(table) {
              return table.name.includes(userInput);
            });
          }, function(text) {
            me.error = text;
          });
        }
        
        function validateName(name) {
          if (!name) { 
            me.nameValid = false;
            return "Name is required";
          }
          
          me.nameValid = true;
          return undefined;
        }
        
        function validateTable(table) {
          if (!table) { 
            me.tableValid = false;
            return "Table is required";
          }
          
          me.tableValid = true;
          return undefined;
        }
        
        initialize();
      } ]);
})();