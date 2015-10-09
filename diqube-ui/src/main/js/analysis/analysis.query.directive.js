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

  angular.module("diqube.analysis").directive("diqubeQuery",
      [ "analysisService", "$timeout", "$log", "analysisStateService", function(analysisService, $timeout, $log, analysisStateService) {
        return {
          restrict: "E",
          scope: {
            query: "=",
            qube: "=",
            analysis: "="
          },
          templateUrl: "analysis/analysis.query.html",
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
            
            $scope.editMode = false;
            $scope.toggleEditMode = toggleEditMode;
            $scope.nameValid = true;
            $scope.diqlValid = true;
            $scope.validateName = validateName;
            $scope.validateDiql = validateDiql;
            $scope.queryCopy = undefined;
            $scope.updateQuery = updateQuery;
            $scope.working = false;
            
            // ===

            if (analysisStateService.pollOpenQueryInEditModeNextTime($scope.query.id)) {
              // query was just created: open in edit mode right away.
              $scope.toggleEditMode();
            }
            
            $scope.$watch("query", executeQuery);
            
            function createDisplayProperties() {
              $scope.displayWidth = "";
              
              if ($scope.query.displayType === "barchart")
                $scope.displayWidth = "width: 600px";
            }

            function integrateQueryResults(results) {
              $scope.$apply(function() {
                $scope.query.results = results;
                if (!$scope.editMode)
                  $scope.exception = results.exception;
                createDisplayProperties();
              });
            }
            
            function executeQuery() {
              analysisService.provideQueryResults($scope.qube, $scope.query, integrateQueryResults).
                then(integrateQueryResults, integrateQueryResults);
            }
            
            function switchQueryDisplayType(newDisplayTypeId) {
              var newQuery = angular.copy($scope.query);
              newQuery.displayType = newDisplayTypeId;
              return analysisService.updateQuery($scope.qube.id, newQuery).then(function() {
                $scope.$digest();
              }).catch(function(text) {
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
            
            function toggleEditMode() {
              $scope.editMode = !$scope.editMode;
              if ($scope.editMode) {
                $scope.queryCopy = angular.copy($scope.query);
                $timeout(function() {
                  var textarea = $("textarea", element);
                  // resize text area so it does not need to be scrolled from the beginning...
                  textarea.height(textarea[0].scrollHeight + 10);
                  textarea.width(textarea[0].scrollWidth + 10);
                }, 0, false);
                $scope.nameValid = true;
                $scope.diqlValid = true;
                $scope.working = false;
              } else {
                executeQuery();
              }
              
              $scope.exception = undefined;
            }
           
            function validateName(name) {
              $scope.nameValid = !!name;
            }
            
            function validateDiql(diql) {
              $scope.diqlValid = !!diql;
            }
            
            function updateQuery(newQuery) {
              $scope.working = true;
              analysisService.updateQuery($scope.qube.id, newQuery).then(function success_(receivedQuery) {
                $scope.$apply(function() {
                  $scope.working = false;
                  $scope.editMode = false;
                });
              }, function failure_(text) {
                $scope.$apply(function() {
                  $scope.working = false;
                  $scope.exception = text;
                  
                  // assume that it's the diql that was invalid.
                  $scope.diqlValid = false;
                });
              });
            }
          }
        };
                
      } ]);
})();