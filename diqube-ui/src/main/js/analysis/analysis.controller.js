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

  angular.module("diqube.analysis").controller("AnalysisCtrl",
      [ "$routeParams", "$scope", "analysisService", function($routeParams, $scope, analysisService) {
        var me = this;

        me.analysisId = $routeParams.analysisId;
        me.title = me.analysisId;
        me.error = undefined;
        me.analysis = undefined;
        me.qubes = undefined;
        me.slices = undefined;
        me.table = undefined;
        
        me.addQube = addQube;
        me.addQuery = addQuery;
        
        // ==
        
        me.loadAnalysis = loadAnalysis;
        me.executeQuery = executeQuery;
        
        
        function initialize() {
          analysisService.loadAnalysis(me.analysisId).then(function success_(analysis) {
            $scope.$apply(function() {
              me.loadAnalysis(analysis);
            });
          }, function failure_(text) {
            $scope.$apply(function() {
              me.loadAnalysis(undefined);
              me.error = text;
            });
          });
        }
        
        initialize();
        
        function loadAnalysis(analysis) {
          if (!analysis) {
            me.analysis = undefined;
            me.title = me.analysisId;
            me.qubes = undefined;
            me.slices = undefined;
            me.table = undefined;
            me.error = undefined;
            return;
          }
          me.analysis = analysis;
          me.title = analysis.name;
          me.qubes = analysis.qubes;
          me.table = analysis.table;
          me.slices = analysis.slices;
          
          me.error = undefined;
          
          for (var qubeIdx in me.qubes) {
            var qube = me.qubes[qubeIdx];
            for (var queryIdx in qube.queries) {
              var query = qube.queries[queryIdx];
              me.executeQuery(qube, query);
            }
          }

        }
        
        function addQube() {
          var slicePromise;
          if (me.analysis.slices.length == 0)
            slicePromise = analysisService.addSlice("slice1");
          else {
            slicePromise = new Promise(function(resolve, reject) {
              resolve(me.analysis.slices[0]);
            });
          }
          
          slicePromise.then(function success_(slice) {
            analysisService.addQube("qube1", slice.id);
          }, function failure_(text) {
            // TODO nicer error?
            me.error = text;
          });
        }
        
        function addQuery(qube) {
          analysisService.addQuery("query1", "select state, count() group by state", qube.id).catch(function(text) {
            // TODO nicer error?
            me.error = text;
          });
        }

        function integrateQueryResults(qube, query, results) {
          query.results = results;
        }
        
        function executeQuery(qube, query) {
          analysisService.provideQueryResults(qube, query, function (results) {
            integrateQueryResults(qube, query, results);
          }).then(function success_(results) {
            integrateQueryResults(qube, query, results);
          }, function failure_(results) {
            integrateQueryResults(qube, query, results);
          })
        }
        
        $scope.$on("$destroy", function() {
          analysisService.unloadAnalysis();
        });
        $scope.$on("analysis:sliceAdded", function() {
        });
        $scope.$on("analysis:qubeAdded", function() {
          me.qubes = me.analysis.qubes;
        });
        $scope.$on("analysis:queryAdded", function(event, data) {
          var qube = me.qubes.filter(function(qube) {
            return qube.id == data.qubeId;
          })[0];
          me.executeQuery(qube, data.query);
        });
      } ]);
})();