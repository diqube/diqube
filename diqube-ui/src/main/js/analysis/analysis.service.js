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
      [ "$log", "$rootScope", "remoteService", "$timeout", "analysisStateService", "analysisExecutionService", "$location",
      function analysisServiceProvider($log, $rootScope, remoteService, $timeout, analysisStateService, analysisExecutionService, $location) {
        var me = this;

        me.loadedAnalysis = undefined;
        me.loadAnalysis = loadAnalysis;
        me.unloadAnalysis = unloadAnalysis;
        me.setLoadedAnalysis = setLoadedAnalysis;
        
        me.addQube = addQube;
        me.addQuery = addQuery;
        me.addSlice = addSlice;
        
        me.updateQuery = updateQuery;
        me.updateQube = updateQube;
        me.updateSlice = updateSlice;
        
        me.removeQuery = removeQuery;
        me.removeQube = removeQube;
        me.removeSlice = removeSlice;
        
        me.provideQueryResults = provideQueryResults;
        
        // newest version number (on server) of the analysis ID of the analysis available in loadedAnalysis.
        me.newestVersionOfAnalysis = undefined;
        
        me.cloneAndLoadCurrentAnalysis = cloneAndLoadCurrentAnalysis;
        
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
          
          // ensure the correct version in the analysis object and in the URL.
          setCurrentAnalysisVersion(analysis.version);
          
          $rootScope.$broadcast("analysis:loaded", analysis);
        }
        
        function loadAnalysis(id, version) {
          if (!me.loadedAnalysis || me.loadedAnalysis.id != id || (version && me.loadedAnalysis.version != version)) {
            return new Promise(function(resolve, reject) {
              var previousAnalysis;
              
              if (me.loadedAnalysis && me.loadedAnalysis.id === id)
                // we load "only" a different version of the same analysis. We could be able to preserve some query
                // results below, so store the previous analysis for comparison!
                previousAnalysis = me.loadedAnalysis;
              else
                previousAnalysis = undefined;
              
              me.loadedAnalysis = undefined;
              me.newestVersionOfAnalysis = undefined;
              
              var executeActualLoading = function() {
                remoteService.execute("analysis", { analysisId : id, analysisVersion: version }, new (function() {
                  this.data = function data_(dataType, data) {
                    if (dataType === "analysis") {
                      if (!me.newestVersionOfAnalysis)
                        // in case we loaded the newest version or version loading had an exception.
                        me.newestVersionOfAnalysis = data.analysis.version;
                      me.setLoadedAnalysis(data.analysis);
                      if (previousAnalysis)
                        // try to preserve some query results.
                        preserveResultsOfPreviousAnalysis(previousAnalysis, me.loadedAnalysis);
                      resolve(me.loadedAnalysis);
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                })());
              }
              
              if (version) {
                // we do NOT load the newest version, but a specific one. Therefore: Check what the newest version is
                // to make it available correctly in me.newestVersionOfAnalysis.
                remoteService.execute("newestAnalysisVersion", { analysisId: id }, new function() {
                  this.data = function data_(dataType, data) {
                    if (dataType === "analysisVersion")
                      me.newestVersionOfAnalysis = data.analysisVersion;
                  }
                  this.exception = function exception_(text) {
                    // Hm, exception when checking version number. Assume we load the newest version, let 
                    // me.newestVersionOfAnalysis stay undefined and executeActualLogging set it.
                    executeActualLoading();
                  }
                  this.done = function done_() {
                    executeActualLoading();
                  }
                }());
              } else
                executeActualLoading();
            });
          } else {
            return new Promise(function(resolve, reject) {
              $timeout(function() {
                // loaded already, publish event anyway
                $rootScope.$broadcast("analysis:loaded", me.loadedAnalysis);
                resolve(me.loadedAnalysis);
              }, 0, false);  
            });
          }
        }
        
        /**
         * Clones the currently loaded analysis version into an analysis that is owned by the currently logged in user.
         * Will load the new analysis after it's created.
         */
        function cloneAndLoadCurrentAnalysis() {
          if (!me.loadedAnalysis)
            return new Promise(function(resolve, reject) {
              resolve(undefined);
            });
          
          return new Promise(function(resolve, reject) {
            remoteService.execute("cloneAnalysis", { 
              analysisId : me.loadedAnalysis.id, 
              analysisVersion: me.loadedAnalysis.version }, 
              new (function() {
                this.data = function data_(dataType, data) {
                  if (dataType === "analysis") {
                    // redirect using $location so everything (all controllers etc.) gets built fresh and the URL is 
                    // correct.
                    $timeout(function() {
                      $location.hash(data.analysis.version);
                      $location.path("analysis/" + data.analysis.id);
                      $rootScope.$digest();
                    }, 0, false);
                    resolve(data.analysis);
                  }
                }
                this.exception = function exception_(text) {
                  reject(text);
                }
              })());
          });
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
            remoteService.execute("createQube", 
                { analysisId: me.loadedAnalysis.id, 
                  analysisVersion: me.loadedAnalysis.version,
                  name: name, 
                  sliceId: sliceId }, 
                new (function() {
                  var resQube = undefined;
                  
                  this.data = function data_(dataType, data) {
                    if (dataType === "qube") {
                      me.initializeReceivedQube(data.qube);
                      me.loadedAnalysis.qubes.push(data.qube);
                      setCurrentAnalysisVersion(data.analysisVersion);
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
            remoteService.execute("createQuery", 
                { analysisId: me.loadedAnalysis.id,
                  analysisVersion: me.loadedAnalysis.version,
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
                      setCurrentAnalysisVersion(data.analysisVersion);
                      
                      analysisStateService.markToOpenQueryInEditModeNextTime(data.query.id);
                      
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
        
        function addSlice(name, manualConjunction, sliceDisjunctions) {
          if (!me.loadedAnalysis) {
            $log.warn("No loaded analysis.");
            return;
          }
          return new Promise(function(resolve, reject) {
            remoteService.execute("createSlice", 
                { analysisId: me.loadedAnalysis.id,
                  analysisVersion: me.loadedAnalysis.version,
                  name: name,
                  manualConjunction: manualConjunction,
                  sliceDisjunctions: sliceDisjunctions
                }, 
                new (function() {
                  var resSlice = undefined;
                  this.data = function data_(dataType, data) {
                    if (dataType === "slice") {
                      me.loadedAnalysis.slices.push(data.slice);
                      setCurrentAnalysisVersion(data.analysisVersion);
                      
                      analysisStateService.markToOpenSliceInEditModeNextTime(data.slice.id);
                      
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
         * Loads a field "$results" into the query object which is updated continuously until it contains the full
         * results of executing the query
         * 
         * For details, see corresponding method in analysis.execution.service.js.
         *  
         * @param qube The qube of the query to execute
         * @param query The query to execute
         * @param intermediateResultsFn function(resultsObj): called when intermediate results are available. Can be undefined. This will only be called asynchronously.
         */
        function provideQueryResults(qube, query, intermediateResultsFn) {
          return analysisExecutionService.provideQueryResults(me.loadedAnalysis.id, me.loadedAnalysis.version, qube, 
              query, intermediateResultsFn);
        }
        
        /**
         * Sends an updated version of a query to the server. Note that the passed query object should not yet be the 
         * one that is reachable from me.loadedAnalysis, as the changes will be incorporated into that object when the
         * resulting query is received from the server after updating.
         * 
         * If possible, the query.$results will be preserved in the new query object, but it could be that they are
         * removed and need to be re-queried using #provideQueryResults.
         */
        function updateQuery(qubeId, query) {
          return new Promise(function(resolve, reject) {
            var cleanDiql = query.diql;
            if (cleanDiql) {
              // Chrome seems to sometimes send bytes "c2 a0" (which is encoded unicode &nbsp;, in unicode \u00a0). 
              // Our parser does not like this, so replace it with a regular space.
              cleanDiql = cleanDiql.replace(/\xc2\xa0/g, " ");
              cleanDiql = cleanDiql.replace(/\u00a0/g, " ");
            }
            remoteService.execute("updateQuery",
                { analysisId: me.loadedAnalysis.id,
                  analysisVersion: me.loadedAnalysis.version,
                  qubeId: qubeId,
                  newQuery: {
                    id: query.id,
                    name: query.name,
                    diql: cleanDiql,
                    displayType: query.displayType
                  }
                }, new (function() {
                  var receivedQuery;
                  this.data = function data_(dataType, data) {
                    if (dataType === "query") {
                      receivedQuery = data.query;
                      setCurrentAnalysisVersion(data.analysisVersion);
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    var replacedQuery = false;
                    for (var qubeIdx in me.loadedAnalysis.qubes) {
                      if (me.loadedAnalysis.qubes[qubeIdx].id === qubeId) {
                        var qube = me.loadedAnalysis.qubes[qubeIdx];
                        for (var queryIdx in qube.queries) {
                          if (qube.queries[queryIdx].id === receivedQuery.id) {
                            var oldQuery = qube.queries[queryIdx]; 
                            qube.queries[queryIdx] = receivedQuery;
                            
                            if (oldQuery.diql == receivedQuery.diql)
                              // preserve the $results we loaded already, if possible!
                              receivedQuery.$results = oldQuery.$results;
                            else
                              // be sure to cancel execution if the query executes based on old properties
                              analysisExecutionService.cancelQueryIfRunning(oldQuery);
                            
                            replacedQuery = true;
                            break;
                          }
                        }
                      }
                      if (replacedQuery)
                        break;
                    }
                    
                    if (!replacedQuery) {
                      $log.warn("Could not find the query that should be replaced by the updated query. " + 
                          "Did the server change the query ID?");
                      reject("Internal error. Please refresh the page.");
                      return;
                    }
                    
                    $rootScope.$broadcast("analysis:queryUpdated", { qubeId: qubeId, query: receivedQuery });
                    resolve(receivedQuery);
                  }
                })());
          });
        }
        
        /**
         * Deletes a query.
         */
        function removeQuery(qubeId, queryId) {
          return new Promise(function(resolve, reject) {
            var qube = me.loadedAnalysis.qubes.filter(function (q) { return q.id === qubeId; })[0];
            var query = qube.queries.filter(function (q) { return q.id === queryId; })[0];
            analysisExecutionService.cancelQueryIfRunning(query);
            
            remoteService.execute("removeQuery", {
              analysisId: me.loadedAnalysis.id,
              analysisVersion: me.loadedAnalysis.version,
              qubeId: qubeId,
              queryId: queryId
            }, new (function() {
              this.data = function data_(dataType, data) {
                if (dataType === "analysisVersion")
                  setCurrentAnalysisVersion(data.analysisVersion);
              }
              this.exception = function exception_(text) {
                reject(text);
              }
              this.done = function done_() {
                var removedQuery = false;
                for (var qubeIdx in me.loadedAnalysis.qubes) {
                  var qube = me.loadedAnalysis.qubes[qubeIdx];
                  
                  if (qube.id !== qubeId)
                    continue;
                  
                  for (var queryIdx in qube.queries) {
                    if (qube.queries[queryIdx].id === queryId) {
                      qube.queries.splice(queryIdx, 1);
                      removedQuery = true;
                      break;
                    }
                  }
                  
                  if (removedQuery) 
                    break;
                }
                
                if (!removedQuery) { 
                  $log.warn("Could not find the query that should have been removed.");
                  reject("Internal error. Please refresh the page.");
                  return;
                }
                
                $rootScope.$broadcast("analysis:queryRemoved", { qubeId: qubeId, queryId: queryId });
                resolve();
              }
            }));
          })
        }
        
        /**
         * Stores an updated qube on the server. Note that the new qube should not be reachable from loadedAnalysis, but
         * this method will do integrate the new qube after the server responded with success.
         * 
         * After the command completed, the queries of the qube might need to be re-executed (if the slice of the qube
         * changed). This method will automatically remove the $results objects in the queries in that case.
         * 
         * Note that this method will update only a few properties of a qube on the server - changes on queries are
         * handled by separate methods!
         */
        function updateQube(newQube) {
          return new Promise(function(resolve, reject) {
            remoteService.execute("updateQube",
                { analysisId: me.loadedAnalysis.id,
                  analysisVersion: me.loadedAnalysis.version,
                  qubeId: newQube.id,
                  qubeName: newQube.name,
                  sliceId: newQube.sliceId
                }, new (function() {
                  var receivedQube = undefined;
                  this.data = function data_(dataType, data) {
                    if (dataType === "qube") {
                      receivedQube = data.qube;
                      setCurrentAnalysisVersion(data.analysisVersion);
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    var foundQubeIdx = undefined;
                    for (var idx in me.loadedAnalysis.qubes) {
                      if (me.loadedAnalysis.qubes[idx].id === receivedQube.id) {
                        foundQubeIdx = idx;
                        break;
                      }
                    }
                    
                    if (foundQubeIdx === undefined) {
                      $log.warn("Could not find the qube that should be replaced by the updated qube. Did the server " +
                          "change the query ID?");
                      reject("Internal error. Please refresh the page.");
                      return;
                    }
                    
                    var oldQube = me.loadedAnalysis.qubes[foundQubeIdx];
                    me.loadedAnalysis.qubes[foundQubeIdx] = receivedQube;

                    // we replaced the whole qube including the $results of all queries (which are empty now again).
                    // If the slice did not change, we can preserve the results! Otherwise we have to re-run the queries.
                    // Be sure to cancel any potentially runnign queries if we effectively delete $results.
                    for (var newQueryIdx in receivedQube.queries) {
                      var newQuery = receivedQube.queries[newQueryIdx];
                      var oldQueryArray = oldQube.queries.filter(function (q) { return q.id === newQuery.id; });
                      if (oldQueryArray && oldQueryArray.length) {
                        if (oldQube.sliceId === receivedQube.sliceId) {
                          // preserve results
                          newQuery.$results = oldQueryArray[0].$results;
                        } else {
                          // Do not preserve results, if executing: cancel!
                          analysisExecutionService.cancelQueryIfRunning(oldQueryArray[0]);
                        }
                      }
                    }
                    
                    $rootScope.$broadcast("analysis:qubeUpdated", { qube: receivedQube });
                    resolve(receivedQube);
                  }
                })());
          });
        }
        
        /**
         * Deletes a qube.
         */
        function removeQube(qubeId) {
          return new Promise(function(resolve, reject) {
            var qube = me.loadedAnalysis.qubes.filter(function (q) { return q.id === qubeId; })[0];
            for (var queryIdx in qube.queries) {
              analysisExecutionService.cancelQueryIfRunning(qube.queries[queryIdx]);
            }
            
            remoteService.execute("removeQube", {
              analysisId: me.loadedAnalysis.id,
              analysisVersion: me.loadedAnalysis.version,
              qubeId: qubeId,
            }, new (function() {
              this.data = function data_(dataType, data) {
                if (dataType === "analysisVersion")
                  setCurrentAnalysisVersion(data.analysisVersion);
              }
              this.exception = function exception_(text) {
                reject(text);
              }
              this.done = function done_() {
                var removedQube = false;
                for (var qubeIdx in me.loadedAnalysis.qubes) {
                  var qube = me.loadedAnalysis.qubes[qubeIdx];
                  
                  if (qube.id === qubeId) {
                    me.loadedAnalysis.qubes.splice(qubeIdx, 1);
                    removedQube = true;
                    break;
                  }
                }
                
                if (!removedQube) { 
                  $log.warn("Could not find the qube that should have been removed.");
                  reject("Internal error. Please refresh the page.");
                  return;
                }
                
                $rootScope.$broadcast("analysis:qubeRemoved", qubeId);
                resolve();
              }
            }));
          })
        }
        
        /**
         * Sends an updated version of a slice to the server. Note that the passed slice object should not yet be the 
         * one that is reachable from me.loadedAnalysis, as the changes will be incorporated into that object when the
         * resulting slice is received from the server after updating.
         * 
         * Note that this method will remove the query.$results objects of all queries that are connected to the slice
         * by their qube (only if slices selection properties changed). The results might therefore need to be
         * re-calculated!
         */
        function updateSlice(slice) {
          return new Promise(function(resolve, reject) {
            remoteService.execute("updateSlice",
                { analysisId: me.loadedAnalysis.id,
                  analysisVersion: me.loadedAnalysis.version,
                  slice: slice
                }, new (function() {
                  var receivedSlice;
                  this.data = function data_(dataType, data) {
                    if (dataType === "slice") { 
                      receivedSlice = data.slice;
                      setCurrentAnalysisVersion(data.analysisVersion);
                    }
                  }
                  this.exception = function exception_(text) {
                    reject(text);
                  }
                  this.done = function done_() {
                    var origSlice = undefined;
                    for (var sliceIdx in me.loadedAnalysis.slices) {
                      if (me.loadedAnalysis.slices[sliceIdx].id === receivedSlice.id) {
                        origSlice = me.loadedAnalysis.slices[sliceIdx];
                        me.loadedAnalysis.slices[sliceIdx] = receivedSlice;
                        break;
                      }
                    }
                    
                    if (!origSlice) {
                      $log.warn("Could not find the slice that should be replaced by the updated slice. " + 
                          "Did the server change the slice ID?");
                      reject("Internal error. Please refresh the page.");
                      return;
                    }
                    
                    if (!angular.equals(origSlice.manualConjunction, receivedSlice.manualConjunction) || 
                        !angular.equals(origSlice.sliceDisjunctions, receivedSlice.sliceDisjunctions)) {
                      for (var qubeIdx in me.loadedAnalysis.qubes) {
                        var qube = me.loadedAnalysis.qubes[qubeIdx];
                        if (qube.sliceId === receivedSlice.id) {
                          // clean $results
                          for (var queryIdx in qube.queries) {
                            qube.queries[queryIdx].$results = undefined;
                            analysisExecutionService.cancelQueryIfRunning(qube.queries[queryIdx]);
                          }
                        }
                      }
                    }
                    
                    $rootScope.$broadcast("analysis:sliceUpdated", receivedSlice);
                    resolve(receivedSlice);
                  }
                })());
          });
        }
        
        /**
         * Deletes a slice.
         */
        function removeSlice(sliceId) {
          return new Promise(function(resolve, reject) {
            remoteService.execute("removeSlice", {
              analysisId: me.loadedAnalysis.id,
              analysisVersion: me.loadedAnalysis.version,
              sliceId: sliceId
            }, new (function() {
              this.data = function data_(dataType, data) {
                if (dataType === "analysisVersion")
                  setCurrentAnalysisVersion(data.analysisVersion);
              }
              this.exception = function exception_(text) {
                reject(text);
              }
              this.done = function done_() {
                var removedSlice = false;
                for (var sliceIdx in me.loadedAnalysis.slices) {
                  var slice = me.loadedAnalysis.slices[sliceIdx];
                  
                  if (slice.id === sliceId) {
                    me.loadedAnalysis.slices.splice(sliceIdx, 1);
                    removedSlice = true;
                    break;
                  }
                }
                
                if (!removedSlice) { 
                  $log.warn("Could not find the slice that should have been removed.");
                  reject("Internal error. Please refresh the page.");
                  return;
                }
                
                $rootScope.$broadcast("analysis:sliceRemoved", sliceId);
                resolve();
              }
            }));
          })
        }
 
        function setCurrentAnalysisVersion(newVersion) {
          me.loadedAnalysis.version = newVersion;
          if (!me.newestVersionOfAnalysis || newVersion > me.newestVersionOfAnalysis)
            me.newestVersionOfAnalysis = newVersion;
          
          $timeout(function() {
            $location.hash(newVersion);
          }, 1);
        }
        
        function initializeReceivedQube(qube) {
          if (!qube.queries)
            qube.queries = [];
        }
        function initializeReceivedSlice(slice) {
          if (!slice.sliceDisjunctions)
            slice.sliceDisjunctions = [];
        }
        
        /**
         * Tries to preserve some query results. This is meaningful if the previousAnalysis is actually just a
         * different version of the same analysis, which means that a lot of queries might actually be equal and we do
         * not need to calculate them anymore. This method identifies the queries that are the same between two analysis
         * and copies potentially available results to the new one.
         */
        function preserveResultsOfPreviousAnalysis(previousAnalysis, newAnalysis) {
          if (!previousAnalysis || !newAnalysis)
            return;
          if (previousAnalysis.id !== newAnalysis.id)
            // does make sense to only compare two versions of the same analysis.
            return;
          
          var equalSliceIds = []; // "equal" means: same restrictions. If the name is different, that does not matter.
          for (var prevSliceIdx in previousAnalysis.slices) {
            var prevSlice = previousAnalysis.slices[prevSliceIdx];
            var newSlices = newAnalysis.slices.filter(function (s) { return s.id === prevSlice.id; });
            if (newSlices && newSlices.length) {
              var newSlice = newSlices[0];
              
              // ok, we have a slice with the same ID in prev and new.
              if (angular.equals(prevSlice.sliceDisjunctions, newSlice.sliceDisjunctions) && 
                  angular.equals(prevSlice.manualConjunction, newSlice.manualConjunction))
                equalSliceIds.push(prevSlice.id);
            }
          }
          
          var preservedQueryResults = 0;
          
          for (var prevQubeIdx in previousAnalysis.qubes) {
            var prevQube = previousAnalysis.qubes[prevQubeIdx];
            if (equalSliceIds.indexOf(prevQube.sliceId) > -1) {
              var newQubes = newAnalysis.qubes.filter(function (q) { return q.id === prevQube.id; });
              if (newQubes && newQubes.length) {
                // Ok, we found a qube that (1) has a slice that is equal in prev and new and (2) that is available in prev and new.
                var newQube = newQubes[0];
                
                for (var prevQueryIdx in prevQube.queries) {
                  var prevQuery = prevQube.queries[prevQueryIdx];
                  
                  if (!prevQuery.$results)
                    // if we do not have any results, we will not be able to copy anything, therefore skip right away.
                    continue;
                  
                  var newQueries = newQube.queries.filter(function (q) { return q.id === prevQuery.id; });
                  if (newQueries && newQueries.length) {
                    var newQuery = newQueries[0];
                    // Ok, we found that both prev and new contain a query with the same Id.
                    
                    if (angular.equals(newQuery.diql, prevQuery.diql)) {
                      // wohoo, we found equal queries in prev and new!
                      newQuery.$results = prevQuery.$results;
                      preservedQueryResults = preservedQueryResults + 1;
                    }
                  }
                }
              }
            }
          }
          
          $log.info("Preserved", preservedQueryResults, "query results when loading different version of same analysis.");
        }
      } ]);
})();