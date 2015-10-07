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
      [ "analysisService", function(analysisService) {
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
              
              if ($scope.query.displayType === "barchart") {
                var nvd3Values = [];
                for (var idx in $scope.query.results.rows) {
                  nvd3Values.push({
                    idx: idx,
                    label: $scope.query.results.rows[idx][0],
                    value: $scope.query.results.rows[idx][1]
                  });
                }
                $scope.displayWidth = "width: 450px";
                
                if (!$scope.nvd3)
                  $scope.nvd3 = {};
                
                $scope.nvd3.options = nvd3BarChartOptions(450, 300, 
                    $scope.query.results.columnNames ? $scope.query.results.columnNames[0] : "", 
                    $scope.query.results.columnNames ? $scope.query.results.columnNames[1] : "");
                
                $scope.nvd3.data = [ {
                      key: "Values",
                      values: nvd3Values
                    } ];
              }
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
            
            function nvd3BarChartOptions(width, height, xAxisLabel, yAxisLabel) { 
              return {
                chart: {
                  type: "discreteBarChart",
                  height: height,
                  width: width,
                  margin : {
                      top: 5,
                      right: 5,
                      bottom: 60,
                      left: 70
                  },
                  x: function(d) { return d.label; },
                  y: function(d) { return d.value; },
                  showValues: false,
                  valueFormat: function(d){
                    return d3.format("d")(d);
                  },
                  transitionDuration: 100,
                  color: function(data) { return "#1f77b4"; },
                  xAxis: {
                      axisLabel: xAxisLabel ? xAxisLabel : "" 
                  },
                  yAxis: {
                      axisLabel: yAxisLabel ? yAxisLabel : "" ,
                      axisLabelDistance: 10,
                      tickFormat: function(d){
                        return d3.format("d")(d);
                      }
                  },
                  duration: 0
                }
              }
            };
          }
        };
                
      } ]);
})();