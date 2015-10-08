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

  angular.module("diqube.analysis").directive("diqubeSlices",
      [ "$timeout", "$log", function($timeout, $log) {
        return {
          restrict: "E",
          scope: {
            analysis: "=",
          },
          templateUrl: "analysis/analysis.slices.html",
          link: function link($scope, element, attrs) {
            $scope.slices = undefined;
            
            // ===
            
            $scope.$watchCollection("analysis.slices", function() {
              if (!$scope.analysis)
                $scope.slices = undefined;
              else
                $scope.slices = $scope.analysis.slices;
            });
            
          }
        };
                
      } ]);
})();