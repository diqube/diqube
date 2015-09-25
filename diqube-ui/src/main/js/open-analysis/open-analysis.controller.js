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
  

  angular.module("diqube.open-analysis", [ "diqube.remote", "ui.bootstrap", "diqube.analysis" ]).controller(
      "OpenAnalysisCtrl", [ "remoteService",  "analysisService", "$scope", "$log", 
      function(remoteService, analysisService, $scope, $log) {
        var me = this;
        
        me.text = "Open Analysis";
        me.title = "";
        me.items = [ ];
        me.reloadAnalysis = reloadAnalysis;
        me.dropdownIsOpen = false;
        me.openAnalysis = openAnalysis;
        me.loading = false;

        // ===
        
        function reloadAnalysis() {
          $log.info("Reloading analysis list from server");
          me.items = [ ];
          me.loading = true;
          remoteService.execute($scope, "listAllAnalysis", null, new (function() {
            this.data = function data_(dataType, data) {
              if (dataType === "analysisRef") {
                me.items.push({
                  name : data.name,
                  id : data.id
                });
              }
            };
            this.done = function done_() {
              me.loading = false;
            };
          })());
        }
        
        function openAnalysis(item) {
          $log.info("Opening ", item);
          me.dropdownIsOpen = false; // close dropdown manually.
          analysisService.loadAnalysis(item.id);
        }
        
        reloadAnalysis();
        
        $scope.$on('analysis:loaded', function(event, data) {
          if (analysisService.loadedAnalysis !== undefined) { 
            me.text = analysisService.loadedAnalysis.name;
            me.title = analysisService.loadedAnalysis.id;
          } else {
            me.text = "Open Analysis";
            me.title = "";
          }
        });
        
        $scope.$on("analysis:created", function(event, data) {
          me.reloadAnalysis();
        });
        
        // needed for dropdown.
        $scope.toggleDropdown = function($event) {
          $event.preventDefault();
          $event.stopPropagation();
        };
      } ]);
})();