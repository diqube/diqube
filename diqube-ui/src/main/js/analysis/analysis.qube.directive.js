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

  angular.module("diqube.analysis").directive("diqubeQube",
      [ "analysisService", "$log", function(analysisService, $log) {
        return {
          restrict: "E",
          scope: {
            qube: "=",
            analysis: "="
          },
          templateUrl: "analysis/analysis.qube.html",
          link: function link($scope, element, attrs) {
            $scope.slice = findSlice();

            $scope.addQuery = addQuery;
            
            $scope.editMode = false;
            // remove mode can only be accessed in edit mode. To exit removeMode, call toggleEditMode therefore.
            $scope.removeMode = false;
            
            $scope.toggleEditMode = toggleEditMode;
            $scope.nameValid = true;
            $scope.validateName = validateName;
            $scope.qubeCopy = undefined;
            $scope.updateQube = updateQube;
            $scope.working = false;
            
            $scope.enterRemoveMode = enterRemoveMode;
            $scope.removeQube = removeQube;
            
            // ====
            
            $scope.$watch("qube", findSlice);
            $scope.$on("analysis:sliceUpdated", function(event, slice) {
              // If the slice changed that we rely on, be sure to digest those changes!
              if (slice.id == $scope.qube.sliceId) {
                findSlice();
                $scope.$digest();
              }
            });
            
            $scope.$on("analysis:queryRemoved", function(event, data) {
              // If the slice changed that we rely on, be sure to digest those changes!
              if (data.qubeId == $scope.qube.id) {
                $scope.$digest();
              }
            });
            
            
            function addQuery() {
              analysisService.addQuery("New query", "", $scope.qube.id).catch(function(text) {
                // TODO nicer error?
                me.error = text;
              });
            }
            
            function findSlice() {
              $scope.slice = $scope.analysis.slices.filter(function(slice) {
                return slice.id === $scope.qube.sliceId;
              })[0];
            }
            
            function toggleEditMode() {
              $scope.editMode = !$scope.editMode;
              if ($scope.editMode) {
                $scope.qubeCopy = angular.copy($scope.qube);
                $scope.nameValid = true;
              } else
                $scope.removeMode = false;
            }
            
            function validateName(name) {
              $scope.nameValid = !!name;
            }
            
            function updateQube(newQube) {
              $scope.working = true;
              analysisService.updateQube(newQube).then(function() {
                // new value will be incorporated automatically, since we watch "qube" and will therefore execute a 
                // $digest.
                $scope.$apply(function() {
                  $scope.working = false;
                  toggleEditMode();                
                });
              }).catch(function(text) {
                $scope.$apply(function() {
                  $scope.working = false;
                  $scope.nameValid = false;
                  $log.warn("Exception when trying to update a qube:", text);
                });
              })
            }
            
            function enterRemoveMode() {
              $scope.removeMode = true;
            }
            
            function removeQube() {
              analysisService.removeQube($scope.qube.id).then(function() {
                // noop as controllers will update and this directive will be removed automatically.
              }).catch(function(text) {
                $log.error(text);
                toggleEditMode();
              });
            }
          }
        };
                
      } ]);
})();