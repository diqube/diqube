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

(function() {

  var testEmptyAnalysisResult = validatedData.data("analysis", {
    analysis: {
      id: "analysisId",
      table: "analysisTable",
      name: "analysisName",
      qubes: [],
      slices: []
    }
  });
  
  var testTwoQubeAnalysisResult = validatedData.data("analysis", {
    analysis: {
      id: "analysisId",
      table: "analysisTable",
      name: "analysisName",
      qubes: [ {
        id: "qubeId1",
        name: "qubeName1",
        sliceId: "sliceId1",
        queries: []
      }, {
        id: "qubeId2",
        name: "qubeName2",
        sliceId: "sliceId1",
        queries: [ {
          id: "queryId2",
          name: "queryName2",
          diql: "queryDiql2",
          displayType: "table"
        } ]
      } ],
      slices: [ {
        id: "sliceId1",
        name: "sliceName1",
        sliceDisjunctions: [],
        manualConjunction: ""
      } ]
    }
  });
  
  var testQubeResult = validatedData.data("qube", {
    qube: {
      id: "qubeId",
      sliceId: "sliceId",
      name: "qubeName",
      queries: [
        {
          id: "queryId",
          name: "queryName",
          diql: "queryDiql",
          displayType: "table"
        }]
    }
  });
  
  var testSliceResult = validatedData.data("slice", {
    slice: {
      id: "sliceId",
      name: "sliceName",
      sliceDisjunctions: [
        {
          fieldName: "fieldA",
          disjunctionValues: ["a", "b", "c"]
        }],
      manualConjunction: ""
    }
  });
  
  var testQueryResult = validatedData.data("query", {
    query: {
      id: "queryId",
      name: "queryName",
      diql: "queryDiql",
      displayType: "table"
    }
  });
  
  var testUpdatedQueryResult = validatedData.data("query", {
    query: {
      id: "queryId2",
      name: "queryName2",
      diql: "queryDiql2",
      displayType: "barchart"
    }
  });
  
  /**
   * Helper function to wait for a specific situation to appear.
   * 
   * This is done using setTimeout to give the JS engine the opportunity to execute the next events in the main-loop 
   * of execution. This is needed e.g. when waiting for results of Promises, although none of the "success" functions 
   * actually does an async call - nevertheless the JS engine does not execute that function right away, but only in 
   * the next main-loop run.
   * 
   * Returns a promise that will be called when the checkFn returned true.
   */
  function waitUntil(checkStr, checkFn) {
    var waitCount = 0;
    return new Promise(function(resolve, reject) {
      var internalCheckFn = function() {
        if (checkFn()) {
          resolve();
          return;
        }
        
        if (waitCount == 10)
          fail("Timed out while waiting for " + checkStr);
        
        waitCount++;
        setTimeout(internalCheckFn, 1);
      };
      
      setTimeout(internalCheckFn, 1);
    });
  }
  
  describe("diqube.analysis module", function() {
    beforeEach(module("diqube.analysis"));
    var $controller, $location;
    beforeEach(inject(function(_$controller_, _$location_){
      $controller = _$controller_;
      $location = _$location_;
    }));
    
    describe("AnalysisCtrl", function() {
      var $scope, controller, mockedLoginStateService;
      beforeEach(function() {
        $scope = new MockedScope();
        mockedLoginStateService = new MockedLoginStateService();
      });

      it("initialized with correct analysis", function(testDone) {
        inject(function($controller) {
          var mockedAnalysisService =  new MockedAnalysisService(
                $scope,
                function() { return testTwoQubeAnalysisResult.analysis; },
                function() { return { percentComplete:100, rows: [[1,2]], columnNames:["colA", "colB"] }; },
                function() { /* qube */ },
                function() { /* query */ },
                function() { /* slice */ });
          
          var controller = $controller("AnalysisCtrl", { 
            $scope: $scope,
            $routeParams: { analysisId: "analysisId" },
            analysisService: mockedAnalysisService,
            loginStateService: mockedLoginStateService });

          waitUntil("Default analysis to be loaded", 
              function() { return controller.analysis == testTwoQubeAnalysisResult.analysis }).then(function() {
                expect(controller.analysis).toBe(testTwoQubeAnalysisResult.analysis);
                expect(controller.error).toBe(undefined);
                expect(controller.title).toEqual(testTwoQubeAnalysisResult.analysis.name);
                expect(controller.analysisId).toEqual("analysisId");
                testDone();
              });
        });
      });
      
      it("addQube adds qube", function(testDone) {
        inject(function($controller) {
          var mockedAnalysisService =  new MockedAnalysisService(
                $scope,
                function() { return testTwoQubeAnalysisResult.analysis; },
                function() { return { percentComplete:100, rows: [[1,2]], columnNames:["colA", "colB"] }; },
                function() { return testQubeResult.qube; },
                function() { /* query */ },
                function() { /* slice */ });
          
          spyOn(mockedAnalysisService, "addQube").and.callThrough();
          spyOn(mockedAnalysisService, "addSlice").and.callThrough();
          
          var controller = $controller("AnalysisCtrl", { 
            $scope: $scope,
            $routeParams: { analysisId: "analysisId" },
            analysisService: mockedAnalysisService,
            loginStateService: mockedLoginStateService });

          waitUntil("Default analysis to be loaded", 
              function() { return controller.analysis == testTwoQubeAnalysisResult.analysis }).then(function() {
            controller.addQube();
            
            waitUntil("addQube has been called on the analysisService", 
                function() { return mockedAnalysisService.addQube.calls.count() == 1 }).then(function() {
                  
                  expect(mockedAnalysisService.addQube.calls.count()).toEqual(1);
                  expect(mockedAnalysisService.addSlice).not.toHaveBeenCalled(); // the slice already exists.
                  expect(controller.error).toBe(undefined);
                  testDone();
                });
              });
        });
      });
      
      it("addQube adds qube and slice if no slice exists", function(testDone) {
        inject(function($controller) {
          var mockedAnalysisService =  new MockedAnalysisService(
                $scope,
                function() { return testEmptyAnalysisResult.analysis; },
                function() { return { percentComplete:100, rows: [[1,2]], columnNames:["colA", "colB"] }; },
                function() { return testQubeResult.qube; },
                function() { /* query */ },
                function() { return testSliceResult.slice; });
          
          spyOn(mockedAnalysisService, "addQube").and.callThrough();
          spyOn(mockedAnalysisService, "addSlice").and.callThrough();
          
          var controller = $controller("AnalysisCtrl", { 
            $scope: $scope,
            $routeParams: { analysisId: "analysisId" },
            analysisService: mockedAnalysisService,
            loginStateService: mockedLoginStateService });

          waitUntil("Default analysis to be loaded", 
              function() { return controller.analysis == testEmptyAnalysisResult.analysis }).then(function() {
            controller.addQube();
            waitUntil("addQube has been called on the analysisService", 
                function() { return mockedAnalysisService.addQube.calls.count() == 1 }).then(function() {
                  expect(mockedAnalysisService.addQube.calls.count()).toEqual(1);
                  expect(mockedAnalysisService.addSlice.calls.count()).toEqual(1);
                  
                  var qubeSliceId = mockedAnalysisService.addQube.calls.argsFor(0)[1];
                  expect(qubeSliceId).toEqual(testSliceResult.slice.id);
                  
                  expect(controller.error).toBe(undefined);
                  testDone();
                });
              });
        });
      });
      
    });
  });
})();