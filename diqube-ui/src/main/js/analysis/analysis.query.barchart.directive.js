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
            $scope.nvd3HtmlId = uuid.v4(); // "ID" the nvd3 HTMLElement has.
            $scope.options = undefined;
            $scope.data = undefined;
            
            // ===
            
            var lastXAxisLabelsUsedForCalculation = undefined;
            var lastYAxisLabelsUsedForCalculation = undefined;

            $scope.$watch("query.$results.columnNames", createDisplayProperties);
            $scope.$watch("query.$results.rows", createDisplayProperties);
            
            // Observes DOM mutations in this directives DOM. If the "svg" element changes, we calculate the 
            // axis-labels that are displayed. If they differ from those that were used to calculate the pixel distances
            // we schedule another calculation. This is only needed as soon as all results are available 
            // (percentComplete == 100). See comment in nvd3BarChartOptions for details.
            var nvd3Observer = new MutationObserver(function(mutations) {
              mutations.forEach(function(mutation) {
                if (mutation.addedNodes && mutation.addedNodes.length && mutation.addedNodes.length > 0) {
                  for (var addedNodeIdx in mutation.addedNodes) {
                    var addedNode = mutation.addedNodes[addedNodeIdx];
                    if (addedNode.localName === "svg") {
                      var xAxisLabelsRendered = findRenderedAxisLabels("x");
                      var yAxisLabelsRendered = findRenderedAxisLabels("y");

                      if ($scope.query.$results && $scope.query.$results.percentComplete === 100) {
                        if (!angular.equals(lastXAxisLabelsUsedForCalculation, xAxisLabelsRendered) || 
                            !angular.equals(lastYAxisLabelsUsedForCalculation, yAxisLabelsRendered)) {
                          // the axis labels that were used for the last pixel calculations in nvd3BarChartOptions were
                          // different than they are now, with 100 percentComplete. We schedule another calculation.
                          $log.debug("Scheduling another chart-calculation for query ", $scope.query.id, 
                              " because the labels changed since the last calculation!");
                          $timeout(
                              function() {
                                $scope.$apply(function() {
                                  createDisplayProperties();
                                });
                              }, 0, false);
                          }
                        }
                      }
                    }
                  }
                });
              });
             
            nvd3Observer.observe($("nvd3", element)[0], { childList: true });
            $scope.$on("$destroy", function() {
              nvd3Observer.disconnect();
            });
            
            /**
             * Re-initialize the properties used by nvd3 with the current data available in $scope.query.
             */
            function createDisplayProperties() {
              var nvd3Values = [];
              for (var idx in $scope.query.$results.rows) {
                nvd3Values.push({
                  idx: idx,
                  label: $scope.query.$results.rows[idx][0],
                  value: $scope.query.$results.rows[idx][1]
                });
              }
              
              $scope.options = nvd3BarChartOptions(600, 300);
              
              $scope.data = [ {
                    key: "Values",
                    values: nvd3Values
                  } ];
              
              // Sometimes nvd3/angular seems to loose the updates. Make sure that after some time there is actually a
              // chart calculated and force the calculation if not.
              var nvd3Scope = $scope.api.getScope();
              var options = $scope.options;
              var data = $scope.data;
              $timeout(function () {
                if (!nvd3Scope.chart) {
                  nvd3Scope.$apply(function() {
                    nvd3Scope.options = options;
                    nvd3Scope.data = data;
                    $scope.api.refresh();
                  });
                }
              }, 100, false);
            }
            
            /**
             * Calculate the "options" for the nvd3 chart. This includes finding which "labels" (or "ticks" in 
             * nvd3-speak) to be shown on the x axis and identify correct pixel-margins etc. for the labels.
             * 
             * @param userWidth: User preference width of the chart in px.
             * @param userHeight: User preference height of the chart in px.
             */
            function nvd3BarChartOptions(userWidth, userHeight) {
              if (!$scope.query.$results || !$scope.query.$results.rows)
                return;
              
              var width = userWidth || 450;
              var height = userHeight || 300;
              
              // find dimensions of text on x axis and the margin we need on the left side (left of y axis). This is
              // calculated by sizes of the currently displayed chart values. This is fine, although we're about to
              // actually show new values in the chart! It is fine, because (1) the height of the font does
              // not change by changing the displayed data and (2) for the calculated width and margin: We will
              // typically calculate the chart options multiple times, as we receive multiple data updates per chart ->
              // there will be a run of this method when all data is loaded already and we can therefore simply use the
              // values of the "previous run" to correctly display this runs' data.
              // For safety, the observer defined above might schedule another calculation at 100%, if the labels we
              // base the calculation on now are different from those displayed at the end.
              var xAxisTextHeight = findXAxisMaxTextHeight();
              var xAxisTextMaxWidth = findAxisTextMaxWidth("x");
              var leftMargin = findLeftMargin(xAxisTextHeight);
              if (!xAxisTextHeight || !xAxisTextMaxWidth || !leftMargin) {
                // use some default values.
                xAxisTextHeight = 14; 
                xAxisTextMaxWidth = 14;
                leftMargin = 14;
              }
              
              lastXAxisLabelsUsedForCalculation = findRenderedAxisLabels("x");
              lastYAxisLabelsUsedForCalculation = findRenderedAxisLabels("y");

              // We distribute the rows along the X axis.
              var numberOfXAxisLabelsToShow = Math.ceil(width / (xAxisTextHeight + 10)); // 10 -> leave some space between labels
              
              var xTickValues = [];
              var indexOffset = 0;
              var idxDelta = Math.ceil($scope.query.$results.rows.length / numberOfXAxisLabelsToShow);
              var lastLabelAdded = false;
              while (!lastLabelAdded) {
                var rowIdx = indexOffset;
                if (rowIdx >= $scope.query.$results.rows.length - 1) {
                  lastLabelAdded = true;
                  rowIdx = $scope.query.$results.rows.length - 1;
                }
                if ($scope.query.$results.rows[rowIdx])
                  xTickValues.push($scope.query.$results.rows[rowIdx][0]);
                
                indexOffset += idxDelta;
              }
              
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
                      axisLabel: $scope.query.$results.columnNames ? $scope.query.$results.columnNames[0] : "",
                      axisLabelDistance: 10,
                      rotateLabels: 315,
                      tickValues: xTickValues
                  },
                  yAxis: {
                      axisLabel: $scope.query.$results.columnNames ? $scope.query.$results.columnNames[1] : "",
                      axisLabelDistance: 10,
                      tickFormat: function(d){
                        return d3.format("d")(d);
                      }
                  },
                  duration: 0
                }
              }
            };
            
            /**
             * Text height of text on the x axis (this means not the height of the box the rotated text spans, but only
             * the height the text had if it weren't rotated).
             */
            function findXAxisMaxTextHeight() {
              if ($("#" + $scope.nvd3HtmlId).length) {
                var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
                var xAxisTextElements = $("svg .nvd3 .nv-x g.tick text", nvd3Element);
                if (!xAxisTextElements.length)
                  return undefined;
                else {
                  var max = 0;
                  for (var i = 0; i < xAxisTextElements.length; i += 1) {
                    var h = xAxisTextElements.get(i).getBBox().height;
                    if (h > max)
                      max = h;
                  }
                  return max;
                }
              }
              
              return undefined;
            }
            
            /**
             * Max text width of text on an axis (this means not the height of the box the rotated text spans, but only
             * the height the text had if it weren't rotated).
             * 
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
             * Calculates the "margin" we should use on the left side of the chart.
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
                
                var svgElement = $("svg", nvd3Element);
                
                // the reference svg "g" element we use to calculate the space needed left of the y axis. Inside this
                // groups coordinate system, the y axis is drawn at "x = 0".
                var referenceGroup = $("g.nvd3 > g", nvd3Element);
                
                var xAxisTextElements = $("svg .nvd3 .nv-x g.tick text", nvd3Element);
                if (!xAxisTextElements.length || !referenceGroup.length || !svgElement.length)
                  return undefined;
                else {
                  var max = 0; // maximum number of pixels needed left of the y axis.
                  for (var i = 0; i < xAxisTextElements.length; i += 1) {
                    var normalizedX = getNormalizedX(svgElement, referenceGroup, xAxisTextElements);
                    
                    // now normalizedX is normalized to the coordinate system where the y axis is drawn at x=0. 
                    // If normalizedX < 0, then we need space left of the y axis to draw this text.
                    if (normalizedX < 0)  {
                      if (-normalizedX > max)
                        max = -normalizedX;
                    }
                  }
                  
                  if (max > defaultLeftMargin)
                    return max;
                  
                  return defaultLeftMargin;
                }
              }
              
              return undefined;
            }
            
            /**
             * Returns a representation of all the labels that are displayed on a specific axis.
             */
            function findRenderedAxisLabels(axis) {
              if (!$("#" + $scope.nvd3HtmlId).length) 
                return undefined;
              var nvd3Element = $("#" + $scope.nvd3HtmlId)[0];
              var axisTextElements = $("svg .nvd3 .nv-" + axis + " g.tick text", nvd3Element);
              if (!axisTextElements.length)
                return undefined;
              return axisTextElements.text();
            }
            
            /**
             * Uses SVG functions to bring an elements "getBBox().x" value into the coordinate system of a referenceElement.
             */
            function getNormalizedX(svgElement, referenceElement, element) {
              var elementTransfromMatrix = element.get(0).getTransformToElement(referenceElement.get(0));

              var svgPoint = svgElement.get(0).createSVGPoint();
              svgPoint.x = element.get(0).getBBox().x;
              svgPoint.y = element.get(0).getBBox().y;
              
              svgPoint = svgPoint.matrixTransform(elementTransfromMatrix);
              
              return svgPoint.x;
            }
          }
        };
                
      } ]);
})();