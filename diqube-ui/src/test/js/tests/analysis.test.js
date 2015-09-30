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
  
  var testEmptyAnalysis = validatedData.data("analysis", {
    analysis: {
      id: "analysisId",
      table: "analysisTable",
      name: "analysisName",
      qubes: [],
      slices: []
    }
  });
  
  var testTwoQubeAnalysis = validatedData.data("analysis", {
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
          diql: "queryDiql2"
        } ]
      } ],
      slices: [ {
        id: "sliceId1",
        name: "sliceName1",
        sliceDisjunctions: []
      } ]
    }
  });
  
  var analysisLoadCommand = validatedData.commandData("analysis", {
    analysisId: "analysisId"
  });
  
  describe("diqube.analysis module", function() {
    var remoteServiceHandlerFn = undefined;
    var rootScope = undefined;
    
    beforeEach(function() {
      remoteServiceHandlerFn = undefined;
      module(function($provide) {
        rootScope = new MockedScope();
        $provide.constant("$rootScope", rootScope);
      });
      module("diqube.analysis"); // load original analysis module.
      module(function($provide) {
        // provide mocked remote service, this will overwrite the orig remote service that was loaded because of the
        // dependency of diqube.analysis to diqube.remote. 
        $provide.factory("remoteService", function() { 
          return new MockedRemoteService(function(res, commandName, commandData) {
            remoteServiceHandlerFn(res, commandName, commandData);
          });
        });
      });
    });
    
    var $controller, $location;
    beforeEach(inject(function(_$controller_, _$location_){
      $controller = _$controller_;
      $location = _$location_;
    }));
    
    describe("analysisService", function(done) {
      var analysisService;
      
      beforeEach(inject(function(_analysisService_){
        analysisService = _analysisService_;
      }));
      
      beforeEach(function() {
        remoteServiceHandlerFn = undefined;
      });

      it("loadAnalysis loads analysis from remote and fires analysis:loaded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysis") {
            expect(commandData).toEqual(analysisLoadCommand);
            res.data("analysis", testEmptyAnalysis);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventAnalysis = undefined;
        rootScope.$on("analysis:loaded", function(event, analysis) {
          eventAnalysis = analysis;
        });
        
        var loadPromise = analysisService.loadAnalysis("analysisId");
        
        loadPromise.then(function(analysis) {
          expect(analysis).toEqual(testEmptyAnalysis.analysis);
          expect(analysisService.loadedAnalysis).toBe(analysis);
          
          expect(eventAnalysis).not.toBe(undefined);
          expect(eventAnalysis).toBe(analysis);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("loadAnalysis does not re-load analysis from remote but fires analysis:loaded event", function(testDone) {
        var numberOfAnalysisRequestedFromRemote = 0;
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysis") {
            numberOfAnalysisRequestedFromRemote++;
            res.data("analysis", testEmptyAnalysis);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var loadPromise = analysisService.loadAnalysis("analysisId");
        
        loadPromise.then(function(firstAnalysis) {
          expect(numberOfAnalysisRequestedFromRemote).toBe(1);
          
          var eventAnalysis = undefined;
          rootScope.$on("analysis:loaded", function(event, analysis) {
            eventAnalysis = analysis;
          });
          
          // re-load
          loadPromise = analysisService.loadAnalysis("analysisId");
          
          loadPromise.then(function(analysis) {
            expect(analysis).toEqual(testEmptyAnalysis.analysis);
            expect(analysisService.loadedAnalysis).toBe(analysis);
            
            expect(eventAnalysis).not.toBe(undefined);
            expect(eventAnalysis).toBe(analysis);
            
            // still "1" as remoteService should not have been called again!
            expect(numberOfAnalysisRequestedFromRemote).toBe(1); 
            testDone();
          }).catch(function (error) {
            fail(error);
          });
        }).catch(function (error) {
          fail(error);
        });
      });
      
      
      it("unloadAnalysis unloads analysis and fires analysis:loaded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysis") {
            res.data("analysis", testEmptyAnalysis);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var loadPromise = analysisService.loadAnalysis("analysisId");
        
        loadPromise.then(function(firstAnalysis) {
          
          var eventAnalysis = { };
          rootScope.$on("analysis:loaded", function(event, analysis) {
            eventAnalysis = analysis;
          });
          
          // unload
          analysisService.unloadAnalysis();
          
          expect(analysisService.loadedAnalysis).toBe(undefined);
          expect(eventAnalysis).toBe(undefined);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
    });
  });
})();