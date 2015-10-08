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
        
        me.addQube = addQube;

        // ==
        
        me.loadAnalysis = loadAnalysis;
        
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
            me.error = undefined;
            return;
          }
          me.analysis = analysis;
          me.title = analysis.name;
          me.error = undefined;
        }
        
        function addQube() {
          var slicePromise;
          if (me.analysis.slices.length == 0)
            slicePromise = analysisService.addSlice("slice1", "round(mul(log(family_income_12_months), 10.)) > 0", []);
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
        
        $scope.$on("$destroy", function() {
          analysisService.unloadAnalysis();
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
        
      } ]);
})();