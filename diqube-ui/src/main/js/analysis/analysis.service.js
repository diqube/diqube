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
  angular.module("diqube.analysis").service("analysisService",
      [ "$log", "$rootScope", "remoteService", function analysisServiceProvider($log, $rootScope, remoteService) {
        var me = this;

        me.loadedAnalysis = undefined;
        me.loadAnalysis = loadAnalysis;
        me.unloadAnalysis = unloadAnalysis;
        me.setLoadedAnalysis = setLoadedAnalysis;
        
        // =====
        
        function setLoadedAnalysis(analysis) {
          me.loadedAnalysis = analysis;
          $rootScope.$broadcast('analysis:loaded');
        }
        
        function loadAnalysis(id) {
          if (!me.loadedAnalysis || me.loadedAnalysis.id != id) {
            return new Promise(function(resolve, reject) {
              remoteService.execute($rootScope, "analysis", { analysisId : id }, new (function() {
                this.data = function data_(dataType, data) {
                  if (dataType === "analysis") {
                    me.loadedAnalysis = data.analysis;
                    $rootScope.$broadcast('analysis:loaded');
                    resolve(me.loadedAnalysis);
                  }
                }
                this.exception = function exception_(text) {
                  reject(text);
                }
              })());
            });
          } else {
            // loaded already, publish event anyway
            $rootScope.$broadcast('analysis:loaded');
            return new Promise(function(resolve, reject) {
              resolve(me.loadedAnalysis);
            });
          }
        }
        
        function unloadAnalysis() {
          me.loadedAnalysis = undefined;
          $rootScope.$broadcast('analysis:loaded');
        }
      } ]);
})();