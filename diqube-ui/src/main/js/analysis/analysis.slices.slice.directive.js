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

  angular.module("diqube.analysis").directive("diqubeSlicesSlice",
      [ "analysisService", "analysisStateService", "$log", function(analysisService, analysisStateService, $log) {
        return {
          restrict: "E",
          scope: {
            slice: "=",
            additionalClass: "@"
          },
          templateUrl: "analysis/analysis.slices.slice.html",
          link: function link($scope, element, attrs) {
            
            $scope.editMode = false;
            $scope.toggleEditMode = toggleEditMode;
            $scope.updateSlice = updateSlice;
            
            $scope.sliceCopy = undefined;
            $scope.nameValid = true;
            $scope.validateName = validateName;
            $scope.working = false;
            $scope.editException = undefined;
            
            // ===

            if (analysisStateService.pollOpenSliceInEditModeNextTime($scope.slice.id)) {
              toggleEditMode();
            }
            
            $scope.$watch("slice", function() { /* force $digest */ });
     
            function toggleEditMode() {
              $scope.editMode = !$scope.editMode;
              if ($scope.editMode) {
                $scope.editException = undefined;
                $scope.nameValid = true;
                $scope.sliceCopy = angular.copy($scope.slice);
              }
            }
            
            function validateName(newName) {
              $scope.nameValid = !!newName;
            }
            
            function updateSlice(newSlice) {
              $scope.working = true;
              analysisService.updateSlice(newSlice).then(function() {
                $scope.working = false;
                // slice data will be re-loaded automatically.
                toggleEditMode();
              }).catch(function(text) {
                $scope.working = false;
                $scope.editException = text;
                $scope.nameValid = undefined;
              });
            }
          }
        };
                
      } ]);
})();