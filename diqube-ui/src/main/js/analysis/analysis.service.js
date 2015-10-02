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
  angular.module("diqube.analysis").service("analysisService",
      [ "$log", "$rootScope", "remoteService", function analysisServiceProvider($log, $rootScope, remoteService) {
        var me = this;

        me.loadedAnalysis = undefined;
        me.loadAnalysis = loadAnalysis;
        me.unloadAnalysis = unloadAnalysis;
        me.setLoadedAnalysis = setLoadedAnalysis;
        
        me.addQube = addQube;
        me.addQuery = addQuery;
        me.addSlice = addSlice;
        
        me.updateQuery = updateQuery;
        
        me.provideQueryResults = provideQueryResults;
        
        // =====
        
        me.initializeReceivedQube = initializeReceivedQube;
        me.initializeReceivedSlice = initializeReceivedSlice;
        
        function setLoadedAnalysis(analysis) {
          me.loadedAnalysis = analysis;
          
          if (!me.loadedAnalysis.qubes)
            me.loadedAnalysis.qubes = [];
          for (var idx in me.loadedAnalysis.qubes)
            me.initializeReceivedQube(me.loadedAnalysis.qubes[idx]);
          
          if (!me.loadedAnalysis.slices)
            me.loadedAnalysis.slices = [];
          for (var idx in me.loadedAnalysis.slices)
            me.initializeReceivedSlice(me.loadedAnalysis.slices[idx]);
          
          $rootScope.$broadcast("analysis:loaded", analysis);
        }
        
        function loadAnalysis(id) {
          if (!me.loadedAnalysis || me.loadedAnalysis.id != id) {
            return new Promise(function(resolve, reject) {
              remoteService.execute($rootScope, "analysis", { analysisId : id }, new (function() {
                this.data = function data_(dataType, data) {
                  if (dataType === "analysis") {
                    me.setLoadedAnalysis(data.analysis);
                    resolve(me.loadedAnalysis);
                  }
                }
                this.exception = function exception_(text) {
                  reject(text);
                }
              })());
            });
          } else {
            // loaded already, publish event anyway
            $rootScope.$broadcast("analysis:loaded", me.loadedAnalysis);
            return new Promise(function(resolve, reject) {
              resolve(me.loadedAnalysis);
            });
          }
        }
        
        function unloadAnalysis() {
          me.loadedAnalysis = undefined;
          $rootScope.$broadcast("analysis:loaded", undefined);
        }
        
        function addQube(name, sliceId) {
          if (!me.loadedAnalysis) {
            $log.warn("No loaded analysis.");
            return;
          }
          return new Promise(function(resolve, reject) {
            remoteService.execute($rootScope, "createQube", 
                { analysisId: me.loadedAnalysis.id, 
                  name: name, 
                  sliceId: sliceId }, 
                new (function() {
                  var resQube = undefined;
                  
                  this.data = function data_(dataType, data) {
                    if (dataType === "qube") {
                      me.initializeReceivedQube(data.qube);
                      me.loadedAnalysis.qubes.push(data.qube);
                      $rootScope.$broadcast("analysis:qubeAdded", data.qube);
                      resQube = data.qube;
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    resolve(resQube);
                  }
                })());
          });
        }
        
        function addQuery(name, diql, qubeId) {
          if (!me.loadedAnalysis) {
            $log.warn("No loaded analysis.");
            return;
          }
          return new Promise(function(resolve, reject) {
            remoteService.execute($rootScope, "createQuery", 
                { analysisId: me.loadedAnalysis.id, 
                  name: name, 
                  qubeId: qubeId,
                  diql: diql }, 
                new (function() {
                  var resQuery = undefined;
                  this.data = function data_(dataType, data) {
                    if (dataType === "query") {
                      var qube = me.loadedAnalysis.qubes.filter(function(q) {
                        return q.id === qubeId;
                      })[0];
                      qube.queries.push(data.query);
                      $rootScope.$broadcast("analysis:queryAdded", { qubeId: qubeId, query: data.query });
                      resQuery = data.query;
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    resolve(resQuery);
                  }
                })());
          });
        }
        
        function addSlice(name) {
          if (!me.loadedAnalysis) {
            $log.warn("No loaded analysis.");
            return;
          }
          return new Promise(function(resolve, reject) {
            remoteService.execute($rootScope, "createSlice", 
                { analysisId: me.loadedAnalysis.id, 
                  name: name }, 
                new (function() {
                  var resSlice = undefined;
                  this.data = function data_(dataType, data) {
                    if (dataType === "slice") {
                      me.loadedAnalysis.slices.push(data.slice);
                      $rootScope.$broadcast("analysis:sliceAdded", data.slice);
                      resSlice = data.slice;
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    resolve(resSlice);
                  }
                })());
          });
        }
        
        /**
         * Loads a field "results" into the query object which is updated continuously until it contains the full
         * results of executing the query. With each new intermediate update available, the optional 
         * intermediateResultsFn will be called.
         * 
         * If there are results available already (query.results !== undefined), the results will not be loaded again.
         * 
         * The results object which is published in the Promise #resolve and #intermediateResultsFn and is set to 
         * query.results looks like this:
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
         * @param intermediateResultsFn function(resultsObj): called when intermediate results are available. Can be undefined.
         */
        function provideQueryResults(qube, query, intermediateResultsFn) {
          if (query.results !== undefined) {
            return new Promise(function(resolve, reject) {
              resolve(query.results);
            })
          }

          query.results = { percentComplete: 0, 
                            rows: undefined, 
                            columnNames: undefined, 
                            exception: undefined };
          if (intermediateResultsFn)
            intermediateResultsFn(query.results);
          
          return new Promise(function(resolve, reject) {
            remoteService.execute($rootScope, "analysisQuery", 
                { analysisId: me.loadedAnalysis.id,
                  qubeId: qube.id,
                  queryId: query.id
                }, new (function() {
                  this.data = function data_(dataType, data) { 
                    if (dataType === "table" && !query.results.exception) {
                      if (data.percentComplete >= query.results.percentComplete) {
                        query.results.rows = data.rows;
                        query.results.columnNames = data.columnNames;
                        query.results.percentComplete = data.percentComplete;
                      }
                      if (intermediateResultsFn)
                        intermediateResultsFn(query.results);
                    }
                  };
                  this.exception = function exception_(text) {
                    query.results.exception = text;
                    reject(query.results);
                  }
                  this.done = function done_() {
                    query.results.percentComplete = 100;
                    resolve(query.results);
                  }
                })());
          });
        }
        
        function updateQuery(qubeId, query) {
          return new Promise(function(resolve, reject) {
            remoteService.execute($rootScope, "updateQuery",
                { analysisId: me.loadedAnalysis.id,
                  qubeId: qubeId,
                  newQuery: {
                    id: query.id,
                    name: query.name,
                    diql: query.diql,
                    displayType: query.displayType
                  }
                }, new (function() {
                  this.data = function data_(dataType, data) {
                    // noop.
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    resolve(query);
                  }
                })());
          });
        }
        
        function initializeReceivedQube(qube) {
          if (!qube.queries)
            qube.queries = [];
        }
        function initializeReceivedSlice(slice) {
          if (!slice.sliceDisjunctions)
            slice.sliceDisjunctions = [];
        }
      } ]);
})();