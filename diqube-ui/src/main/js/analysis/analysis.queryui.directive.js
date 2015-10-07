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
            
            $scope.nvd3HtmlId = uuid.v4();
            
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
                $scope.displayWidth = "width: 600px";
                
                if (!$scope.nvd3)
                  $scope.nvd3 = {};
                
                $scope.nvd3.options = nvd3BarChartOptions(600, 300, 
                    $scope.query.results.columnNames ? $scope.query.results.columnNames[0] : "", 
                    $scope.query.results.columnNames ? $scope.query.results.columnNames[1] : "",
                    $scope.query.results);
                
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
            
            function nvd3BarChartOptions(userWidth, userHeight, xAxisLabel, yAxisLabel, queryResults) {
              if (!queryResults || !queryResults.rows)
                return;
              
              var width = userWidth || 450;
              var height = userHeight || 300;
              
              // find dimensions of text on x axis and the margin we need on the left side (left of y axis). This is
              // calculated by sizes of the currently displayed chart values. This is fine, although we're about to
              // actually change the values displayed in the chart! This is true, because (1) the height of the font does
              // not change by changing the displayed data and (2) for the calculated width and margin: We will
              // typically calculate the chart options multiple times, as we receive multiple data updates per chart ->
              // there will be a run of this method when all data is loaded already and we can therefore simply use the
              // values of the "previous run" to correctly display this runs' data.
              var xAxisTextHeight = findXAxisTextHeight();
              var xAxisTextMaxWidth = findAxisTextMaxWidth("x");
              var leftMargin = findLeftMargin(xAxisTextHeight);
              if (!xAxisTextHeight || !xAxisTextMaxWidth || !leftMargin) {
                // we did not find one, perhaps there is no chart yet. Proceed now with a default value and remember to
                // retry soon -> at some point the real chart will be available!
                $timeout(function() {
                  $scope.$apply(function() {
                    createDisplayProperties();
                  });
                }, 10, false);
                xAxisTextHeight = 14; // use some default value.
                xAxisTextMaxWidth = 14;
                leftMargin = 14;
              }
              
              var xAxisLabelsRendered = findRenderedAxisLabels("x");
              var yAxisLabelsRendered = findRenderedAxisLabels("y");

              // TODO validate that we did use the same labels to render or retry. 
              
              // We distribute the rows along the X axis.
              var numberOfXAxisLabelsToShow = Math.ceil(width / (xAxisTextHeight + 10)); // 10 -> leave some space between labels
              
              var xTickValues = [];
              var indexOffset = 0;
              var idxDelta = queryResults.rows.length / numberOfXAxisLabelsToShow;
              var lastLabelAdded = false;
              for (var i = 0; i < numberOfXAxisLabelsToShow; i += 1) {
                var rowIdx = Math.round(indexOffset);
                if (rowIdx >= queryResults.rows.length - 1) {
                  lastLabelAdded = true;
                  rowIdx = queryResults.rows.length - 1;
                }
                if (queryResults.rows[rowIdx])
                  xTickValues.push(queryResults.rows[rowIdx][0]);
                
                indexOffset += idxDelta;
              }
              
              if (!lastLabelAdded)
                xTickValues.push(queryResults.rows[queryResults.rows.length - 1][0]);
              
              var xAxisLabelsHeight = Math.cos(Math.PI / 4) * xAxisTextMaxWidth; // 45° angle.
              
              return {
                chart: {
                  type: "discreteBarChart",
                  height: userHeight,
                  width: userWidth,
                  margin : {
                      top: 5,
                      right: 5,
                      bottom: xAxisLabelsHeight + 10 + xAxisTextHeight + 20, // use "xAxisTextHeight" as height of axis text
                      left: leftMargin
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
                      axisLabel: xAxisLabel ? xAxisLabel : "",
                      axisLabelDistance: 10,
                      rotateLabels: 315,
                      tickValues: xTickValues
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
            
            function findXAxisTextHeight() {
              if ($("#" + $scope.nvd3HtmlId).length) {
                var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
                var xAxisTextElements = $("svg .nvd3 .nv-x g.tick text", nvd3Element);
                if (!xAxisTextElements.length)
                  return undefined;
                else
                  return xAxisTextElements.get(0).getBBox().height;
              }
              
              return undefined;
            }
            
            /**
             * @param axis either "x" or "y".
             */
            function findAxisTextMaxWidth(axis) {
              if ($("#" + $scope.nvd3HtmlId).length) {
                var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
                var xAxisTextElements = $("svg .nvd3 .nv-" + axis + " g.tick text", nvd3Element);
                if (!xAxisTextElements.length)
                  return undefined;
                else {
                  var max = 0;
                  for (var i = 0; i < xAxisTextElements.length; i += 1) {
                    var w = xAxisTextElements.get(i).getBBox().width;
                    if (w > max)
                      max = w;
                  }
                  return max;
                }
              }
              
              return undefined;
            }

            /**
             * Calculates the "margin" we should use on the left side.
             * 
             * This is slightly complex, as we need to take care of two sizes: 
             * (1) The length of the labels on the y axis
             * (2) The size the 45°-rotated labels on the x axis need. Imagine the case where the left-most label on the
             *     x axis is so long that it requires more space to the left of the chart than the y-axis labels.
             */
            function findLeftMargin(xAxisTextHeight) {
              var normalMaxWidth = findAxisTextMaxWidth("y");
              if (!normalMaxWidth)
                return undefined;
              
              // this value is the margin we'd use if we only inspect the labels on the y axis.
              var defaultLeftMargin = normalMaxWidth + 10 + xAxisTextHeight + 20;
              
              // now check the labels on the x-axis and calculate the number of pixels these labels need "left of the y axis".
              if ($("#" + $scope.nvd3HtmlId).length) {
                var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
                var xAxisTextElements = $("svg .nvd3 .nv-x g.tick text", nvd3Element);
                if (!xAxisTextElements.length)
                  return undefined;
                else {
                  var firstX = undefined; // "x" value of leftmost xAxis label.
                  var max = 0; // maximum number of pixels needed left of the y axis.
                  for (var i = 0; i < xAxisTextElements.length; i += 1) {
                    var el = xAxisTextElements.get(i);
                    var w = el.getBBox().width;
                    if (!firstX)
                      firstX = el.getBBox().x;
                    
                    // Number of px that this text reaches to the left in scope of the left-most "x" of any label on the X axis.
                    var spaceNeededLeftOfYAxis = (firstX - el.getBBox().x) + Math.round(Math.cos(Math.PI / 4) * w); 
                    
                    if (spaceNeededLeftOfYAxis > max)
                      max = spaceNeededLeftOfYAxis;
                  }
                  
                  if (max > defaultLeftMargin)
                    return max;
                  
                  return defaultLeftMargin;
                }
              }
              
              return undefined;
            }
            
            function findRenderedAxisLabels(axis) {
              if (!$("#" + $scope.nvd3HtmlId).length) 
                return undefined;
              var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
              var axisTextElements = $("svg .nvd3 .nv-" + axis + " g.tick text", nvd3Element);
              if (!axisTextElements.length)
                return undefined;
              return axisTextElements.text();
            }
          }
        };
                
      } ]);
})();