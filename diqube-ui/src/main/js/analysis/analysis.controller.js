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
      [ "$routeParams", "$scope", "analysisService", "loginStateService", "$location", "$timeout",
      function($routeParams, $scope, analysisService, loginStateService, $location, $timeout) {
        var me = this;

        me.analysisId = $routeParams.analysisId;
        me.analysisVersion = $location.hash();
        me.title = me.analysisId;
        me.error = undefined;
        me.analysis = undefined;
        
        me.showNewerVersionWarning = showNewerVersionWarning;
        me.newestVersionNumber = newestVersionNumber;
        
        me.showDifferentOwnerWarning = showDifferentOwnerWarning;
        
        me.isWritable = isWritable;
        
        me.addQube = addQube;
        me.addSlice = addSlice;
        
        me.cloneAndLoadCurrentAnalysis = cloneAndLoadCurrentAnalysis;

        // ==
        
        me.loadAnalysis = loadAnalysis;
        
        // listen if the hash changes. The controller is not reloaded by the routeProvider in that case!
        $scope.$on("$routeUpdate", function () {
          if ($location.hash() !== me.analsisVersion) {
            me.analysisVersion = $location.hash();
            initialize();
          }
        });
        
        function initialize() {
          if (!loginStateService.isTicketAvailable()) {
            loginStateService.loginAndReturnHere();
            return;
          }
          
          analysisService.loadAnalysis(me.analysisId, me.analysisVersion).then(function success_(analysis) {
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
            me.analysisVersion = undefined;
            me.title = me.analysisId;
            me.error = undefined;
            return;
          }
          me.analysis = analysis;
          me.analysisVersion = analysis.version;
          me.title = analysis.name;
          me.error = undefined;
        }
        
        function showNewerVersionWarning() {
          return me.analysis && 
                 me.analysis.version < analysisService.newestVersionOfAnalysis &&
                 isWritable();
        }
        
        function newestVersionNumber() {
          return analysisService.newestVersionOfAnalysis;
        }
        
        function isWritable() {
          return me.analysis &&
                 // only if we're logged in with the same user that owns the analysis, we will be able to execute any
                 // remote calls that change the analysis.
                 loginStateService.username === me.analysis.user;
        }
        
        function showDifferentOwnerWarning() {
          return me.analysis && !isWritable();
        }
        
        function addQube() {
          var slicePromise;
          if (me.analysis.slices.length == 0)
            slicePromise = analysisService.addSlice("Default slice", "", []);
          else {
            slicePromise = new Promise(function(resolve, reject) {
              resolve(me.analysis.slices[0]);
            });
          }
          
          slicePromise.then(function success_(slice) {
            analysisService.addQube("New qube", slice.id);
          }, function failure_(text) {
            // TODO nicer error?
            me.error = text;
          });
        }
        
        function addSlice() {
          analysisService.addSlice("New slice", "", []);
        }
        
        function cloneAndLoadCurrentAnalysis() {
          analysisService.cloneAndLoadCurrentAnalysis();
        }
        
        $scope.$on("$destroy", function() {
          $timeout(function() {
            analysisService.unloadAnalysis();
          }, 0, false);
        });
        $scope.$on("analysis:loaded", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:sliceAdded", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:qubeAdded", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:queryAdded", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:queryUpdated", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:queryRemoved", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:qubeUpdated", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:qubeRemoved", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:sliceUpdated", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
        $scope.$on("analysis:sliceRemoved", function() {
          // make sure this scope digests the new object. As this controller references the same analysis object as the
          // analysisService, the new object is already integrated into the analysis of this controller.
          $scope.$digest();
        });
      } ]);
})();