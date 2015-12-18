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
            // remove mode can only be accessed in edit mode. Therefore, call toggleEditMode to exit remove mode.
            $scope.removeMode = false;
            
            $scope.toggleEditMode = toggleEditMode;
            $scope.enterRemoveMode = enterRemoveMode;
            
            $scope.updateSlice = updateSlice;
            
            $scope.collapsed = true;
            $scope.toggleCollapsed = toggleCollapsed; 
            
            $scope.sliceCopy = undefined;
            $scope.nameValid = true;
            $scope.validateName = validateName;
            $scope.isDisjunctionValueValid = isDisjunctionValueValid; 
            $scope.working = false;
            $scope.editException = undefined;
            
            $scope.removeDisjunctionValue = removeDisjunctionValue;
            $scope.addDisjunctionValue = addDisjunctionValue;
            $scope.addDisjunctionField = addDisjunctionField;
            $scope.removeDisjunctionField = removeDisjunctionField;
            
            $scope.removeSlice = removeSlice;
            
            $scope.exception = undefined;
            
            $scope.drop = drop;
            
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
              } else {
                $scope.removeMode = false;
              }
              
              $scope.exception = undefined;
            }
            
            function validateName(newName) {
              $scope.nameValid = !!newName;
            }
            
            function isDisjunctionValueValid(disjValue) {
              return !!disjValue;
            }
            
            function updateSlice(newSlice) {
              // do not continue if any disjunction value is invalid, as server might not be able to handle empty values
              // nicely.
              for (var disjIdx in newSlice.sliceDisjunctions) {
                var disj = newSlice.sliceDisjunctions[disjIdx];
                if (disj.disjunctionValues.filter(function(val) { return !isDisjunctionValueValid(val); }).length > 0)
                  return;
              }
              
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
            
            function removeDisjunctionValue(slice, disjunctionIndex, valueIndex) {
              slice.sliceDisjunctions[disjunctionIndex].disjunctionValues.splice(valueIndex, 1);
            }
            
            function addDisjunctionValue(slice, disjunctionIndex) {
              slice.sliceDisjunctions[disjunctionIndex].disjunctionValues.push("");
            }
            
            function addDisjunctionField(slice, fieldName) {
              if (!fieldName)
                return;
              
              slice.sliceDisjunctions.push({
                fieldName: fieldName,
                disjunctionValues: []
              });
            }
            
            function removeDisjunctionField(slice, disjunctionIndex) {
              slice.sliceDisjunctions.splice(disjunctionIndex, 1);
            }
            
            function toggleCollapsed() {
              $scope.collapsed = !$scope.collapsed; 
            }
            
            function enterRemoveMode() {
              $scope.removeMode = true;
            }
            
            function removeSlice() {
              analysisService.removeSlice($scope.slice.id).then(function() {
                // noop as the other controllers/directives will update automatically and remove this directive.
              }).catch(function(text) {
                $scope.$apply(function() {
                  $scope.exception = text;
                })
              })
            }
            
            function drop(dragDropElement) {
              var sliceCopy = angular.copy($scope.slice);
              var availableDisjunctions = 
                sliceCopy.sliceDisjunctions.filter(function (d) { return d.fieldName === dragDropElement.data.field });
              
              if (availableDisjunctions && availableDisjunctions.length) {
                availableDisjunctions[0].disjunctionValues.push(dragDropElement.data.value);
              } else {
                sliceCopy.sliceDisjunctions.push({
                  fieldName: dragDropElement.data.field,
                  disjunctionValues: [ dragDropElement.data.value ]
                });
              }
              
              $scope.working = true;
              analysisService.updateSlice(sliceCopy).then(function() {
                $scope.working = false;
              }).catch(function(text) {
                $scope.working = false;
                $scope.editException = text;
              });
              return true;
            }
          }
        };
                
      } ]);
})();