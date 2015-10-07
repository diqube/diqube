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

  angular.module("diqube.analysis").directive("diqubeQueryUi",
      [ "analysisService", "$timeout", "$log",  function(analysisService, $timeout, $log) {
        return {
          restrict: "E",
          scope: {
            query: "=",
            qube: "=",
            analysis: "="
          },
          templateUrl: "analysis/analysis.queryui.html",
          link: function link($scope, element, attrs) {
            $scope.validQueryDisplayTypes = [ 
              { 
                id: "table",
                icon: "fa-table",
                title: "Table"
              }, {
                id: "barchart",
                icon: "fa-bar-chart",
                title: "Bar Chart"
              } ];
            $scope.switchQueryDisplayType = switchQueryDisplayType;
            $scope.getDisplayTypeOptions = getDisplayTypeOptions; 
            
            $scope.exception = undefined;
            
            // ===
            
            executeQuery();

            function createDisplayProperties() {
              $scope.displayWidth = "";
              
              if ($scope.query.displayType === "barchart")
                $scope.displayWidth = "width: 600px";
            }

            function integrateQueryResults(results) {
              $scope.$apply(function() {
                $scope.query.results = results;
                $scope.exception = results.exception;
                createDisplayProperties();
              });
            }
            
            function executeQuery() {
              analysisService.provideQueryResults($scope.qube, $scope.query, integrateQueryResults).
                then(integrateQueryResults, integrateQueryResults);
            }
            
            function switchQueryDisplayType(newDisplayTypeId) {
              $scope.query.displayType = newDisplayTypeId;
              createDisplayProperties();
              return analysisService.updateQuery($scope.qube.id, $scope.query).catch(function(text) {
                $scope.$apply(function() {
                  $scope.exception = text;
                });
              });
            }
            
            function getDisplayTypeOptions(displayTypeId) {
              return $scope.validQueryDisplayTypes.filter(function(details) {
                return details.id == displayTypeId;
              })[0];
            }
            
          }
        };
                
      } ]);
})();