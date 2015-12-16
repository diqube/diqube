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
  
  angular.module("diqube.analysis").directive("diqubeQueryBarChart",
      [ "$timeout", "$log", function($timeout, $log) {
        return {
          restrict: "E",
          scope: {
            query: "=",
          },
          templateUrl: "analysis/analysis.query.barchart.html",
          link: function link($scope, element, attrs) {
            $scope.chartHtmlId = uuid.v4(); // "ID" the canvas HTMLElement has.
            $scope.chartWidth = 600;
            $scope.chartHeight = 300;

            // ===
            
            $scope.$watch("query.$results.columnNames", updateData);
            $scope.$watch("query.$results.rows", updateData);
            
            $scope.chart = undefined;
            $scope.initialData = undefined;
            $scope.initialLabels = undefined;
            
            $timeout(function() {
              var ctx = document.getElementById($scope.chartHtmlId);
              var data, labels;
              if ($scope.initialData) {
                // we have initial data already, so use that!
                data = $scope.initialData;
                $scope.initialData = undefined;
                labels = $scope.initialLabels;
                $scope.initialLabels = undefined;
              } else {
                // no data available. Use empty data. This will be updated by #updateData.
                data = [];
                labels = [];
              }
              $scope.chart = new Chart(ctx, {
                type: "bar",
                data: {
                    labels: labels,
                    datasets: [{
                            label: "Value",
                            backgroundColor: "#1f77b4",
                            data: data
                    }]
                },
                options:{
                    scales: {
                        yAxes: [{
                                ticks: {
                                    beginAtZero:true,
                                    fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif',
                                    fontColor: "#333",
                                },
                            }],
                        xAxes: [{
                              ticks: {
                                  fontFamily: '"Helvetica Neue",Helvetica,Arial,sans-serif',
                                  fontColor: "#333",
                              },
                          }]
                    },
                    responsive: true,
                    maintainAspectRatio: false
                }
              });
            }, 0, false);
            
            function updateData() {
              var targetData, targetLabels;
              if ($scope.chart) {
                // update chart directly.
                $scope.chart.data.datasets[0].data = [];
                targetData = $scope.chart.data.datasets[0].data;
                $scope.chart.data.labels = [];
                targetLabels = $scope.chart.data.labels;
              } else {
                // store data in "initialData" in case this is called before we have a chart.
                $scope.initialData = [];
                targetData = $scope.initialData;
                $scope.initialLabels = [];
                targetLabels = $scope.initialLabels;
              }
              
              for (var idx in $scope.query.$results.rows) {
                targetData.push($scope.query.$results.rows[idx][1]);
                targetLabels.push($scope.query.$results.rows[idx][0]);
              }
              
              if ($scope.chart)
                $scope.chart.update();
            }
          }
        };
                
      } ]);
})();