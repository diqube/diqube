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
"use strict";

var MockedAnalysisService = (function() {
  /**
   * 
   * @param $rootScope can be a function returning the actual rootScope (used for late binding).
   */
  function MockedAnalysisService($rootScope, analysisLoadFn, queryResultsFn, qubeFn, queryFn, sliceFn) {
    var me = this;
    
    me.loadedAnalysis = undefined;
    me.loadAnalysis = loadAnalysis;
    me.unloadAnalysis = unloadAnalysis;
    me.setLoadedAnalysis = setLoadedAnalysis;
    
    me.addQube = addQube;
    me.addQuery = addQuery;
    me.addSlice = addSlice;
    
    me.updateQuery = updateQuery;
    
    me.provideQueryResults = provideQueryResults;
    
    // ====
    
    function setLoadedAnalysis(analysis) {
      me.loadedAnalysis = analysis;
      if (typeof $rootScope === "function")
        $rootScope().$broadcast("analysis:loaded", analysis);
      else
        $rootScope.$broadcast("analysis:loaded", analysis);
    }
    
    function unloadAnalysis() {
      me.loadedAnalysis = undefined;
      if (typeof $rootScope === "function")
        $rootScope().$broadcast("analysis:loaded", undefined);
      else
        $rootScope.$broadcast("analysis:loaded", undefined);
    }
    
    function loadAnalysis(id) {
      return new Promise(function (resolve, reject) {
        setLoadedAnalysis(analysisLoadFn(id));
        resolve(me.loadedAnalysis);
      });
    }
    
    function provideQueryResults(qube, query, intermediaryResultsFn) {
      query.results = queryResultsFn(query);
      return new Promise(function(resolve, reject) {
        resolve(query.results);
      });
    }
    
    function addQube(name, sliceId) {
      var qube = qubeFn(name, sliceId);
      me.loadedAnalysis.qubes.push();
      if (typeof $rootScope === "function")
        $rootScope().$broadcast("analysis:qubeAdded", qube);
      else
        $rootScope.$broadcast("analysis:qubeAdded", qube);
      return new Promise(function(resolve, reject) {
        resolve(qube);
      });
    }
    
    function addQuery(name, diql, qubeId) {
      var query = queryFn(name, diql, qubeId);
      var qube = me.loadedAnalysis.qubes.filter(function(qube) { return qube.id === qubeId; })[0];
      qube.queries.push(query);
      if (typeof $rootScope === "function")
        $rootScope().$broadcast("analysis:queryAdded", { qubeId: qubeId, query: query });
      else
        $rootScope.$broadcast("analysis:queryAdded", { qubeId: qubeId, query: query });
      return new Promise(function(resolve, reject) {
        resolve({ qubeId: qubeId, query: query });
      });
    }
    
    function addSlice(name) {
      var slice = sliceFn(name);
      me.loadedAnalysis.slices.push(slice);
      if (typeof $rootScope === "function")
        $rootScope().$broadcast("analysis:sliceAdded", slice);
      else
        $rootScope.$broadcast("analysis:sliceAdded", slice);
      return new Promise(function(resolve, reject) {
        resolve(slice);
      });
    }
    
    function updateQuery(qubeId, query) {
      return new Promise(function(resolve, reject) {
        var query = queryFn(undefined, undefined, qubeId);
        resolve(query);
      });
    }
  }
  return MockedAnalysisService;
})();