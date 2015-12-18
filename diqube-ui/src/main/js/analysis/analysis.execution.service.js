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
  angular.module("diqube.analysis").service("analysisExecutionService",
      [ "$log", "$rootScope", "remoteService", "$timeout",
      function analysisServiceProvider($log, $rootScope, remoteService, $timeout, analysisStateService) {
        var me = this;

        me.provideQueryResults = provideQueryResults;
        me.cancelQueryIfRunning = cancelQueryIfRunning; 
        
        // =====
        
        me.$runningQueries = [];
        
        /**
         * Loads a field "$results" into the query object which is updated continuously until it contains the full
         * results of executing the query. With each new intermediate update available, the optional 
         * intermediateResultsFn will be called.
         * 
         * If there are results available already (query.$results !== undefined), the results will not be loaded again.
         * 
         * The results object which is published in the Promise #resolve and #intermediateResultsFn and is set to 
         * query.$results looks like this:
         * 
         * {
         * percentComplete [number]: 0-100 percent complete of query.
         * rows [array of array of cell values]: The cell values of the result table. Outer arrays are rows, inner are
         *                                       indexed the same way as "columnNames".
         * columnNames [array of string]: the column names. 
         * exception [string]: If set, an exception occurred executing the query. Display the text and ignore other 
         *                     values.
         * }
         * 
         * Note that the returned Promise will return one of those "result objects" even on a call to "reject"!
         * 
         * @param qube The qube of the query to execute
         * @param query The query to execute
         * @param intermediateResultsFn function(resultsObj): called when intermediate results are available. Can be undefined. This will only be called asynchronously.
         */
        function provideQueryResults(analysisId, analysisVersion, qube, query, intermediateResultsFn) {
          if (query.$results !== undefined) {
            return new Promise(function(resolve, reject) {
              resolve(query.$results);
            })
          }
          
          // If someone just set query.$results to undefined and wants to re-execute, there might be another query 
          // running anyway. Cancel that one.
          cancelQueryIfRunning(query);

          query.$results = { percentComplete: 0, 
                            rows: undefined, 
                            columnNames: undefined, 
                            exception: undefined };
          if (intermediateResultsFn) {
            // use timeout to call intermediateResultsFn asynchronously. This is needed to not mess up with $scope.$apply calls...
            $timeout(function() {
              intermediateResultsFn(query.$results);              
            }, 0, false);
          }
          
          return new Promise(function(resolve, reject) {
            var requestId = remoteService.execute("analysisQuery", 
                { analysisId: analysisId,
                  analysisVersion: analysisVersion,
                  qubeId: qube.id,
                  queryId: query.id
                }, new (function() {
                  this.data = function data_(dataType, data) { 
                    if (dataType === "table" && !query.$results.exception) {
                      if (data.percentComplete >= query.$results.percentComplete) {
                        query.$results.rows = data.rows;
                        query.$results.columnNames = data.columnNames;
                        query.$results.columnRequests = data.columnRequests;
                        query.$results.percentComplete = data.percentComplete;
                      }
                      if (intermediateResultsFn)
                        intermediateResultsFn(query.$results);
                    }
                  };
                  this.exception = function exception_(text) {
                    var foundIdx = undefined;
                    for (var idx in me.$runningQueries) {
                      if (me.$runningQueries[idx].requestId === requestId) {
                        foundIdx = idx;
                        break;
                      }
                    }
                    
                    if (foundIdx !== undefined) {
                      me.$runningQueries.splice(foundIdx, 1);
                    }
                    
                    query.$results.exception = text;
                    reject(query.$results);
                  }
                  this.done = function done_() {
                    var foundIdx = undefined;
                    for (var idx in me.$runningQueries) {
                      if (me.$runningQueries[idx].requestId === requestId) {
                        foundIdx = idx;
                        break;
                      }
                    }
                    
                    if (foundIdx !== undefined) {
                      me.$runningQueries.splice(foundIdx, 1);
                    }
                    
                    query.$results.percentComplete = 100;
                    resolve(query.$results);
                  }
                })());
            
            me.$runningQueries.push({
              queryId: query.id,
              requestId: requestId
            });
          });
        }
        
        /**
         * Cancels the execution of a query if there is one registered currently.
         */
        function cancelQueryIfRunning(query) {
          var foundIdx = undefined;
          for (var idx in me.$runningQueries) {
            if (me.$runningQueries[idx].queryId === query.id) {
              foundIdx = idx;
              break;
            }
          }
          
          if (foundIdx === undefined)
            return;
          
          remoteService.cancel(me.$runningQueries[foundIdx].requestId);
          me.$runningQueries.splice(foundIdx, 1);
          
          if (query.$results !== undefined)
            query.$results.exception = "Cancelled.";
        }
      } ]);
})();