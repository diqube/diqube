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
  
  var testQubeResult = validatedData.data("qube", {
    qube: {
      id: "qubeId",
      sliceId: "sliceId",
      name: "qubeName",
      queries: [
        {
          id: "queryId",
          name: "queryName",
          diql: "queryDiql"
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
        }]
    }
  });
  
  var testQueryResult = validatedData.data("query", {
    query: {
      id: "queryId",
      name: "queryName",
      diql: "queryDiql"
    }
  });
  
  
  var testTableResult1 = validatedData.data("table", {
    columnNames: [ "colA", "colB" ],
    rows: [
      [ 1, 1 ],
      [ 101, 101 ]
           ],
    percentComplete: 50
  });
  
  var testTableResult2 = validatedData.data("table", {
    columnNames: [ "colA", "colB" ],
    rows: [
      [ 2, 2 ],
      [ 102, 102 ]
           ],
    percentComplete: 99
  });
  
  var analysisLoadCommand = validatedData.commandData("analysis", {
    analysisId: "analysisId"
  });
  
  var createQubeCommand = validatedData.commandData("createQube", {
    analysisId: "analysisId", 
    name: "qubeName", 
    sliceId: "sliceId"
  });
  
  var createSliceCommand = validatedData.commandData("createSlice", {
    analysisId: "analysisId", 
    name: "sliceName", 
  });
  
  var createQueryCommand = validatedData.commandData("createQuery", {
    analysisId: "analysisId", 
    qubeId: "qubeId1",
    name: "queryName",
    diql: "queryDiql"
  });
  
  var analysisQueryCommand = validatedData.commandData("analysisQuery", {
    analysisId: "analysisId", 
    qubeId: "qubeId2",
    queryId: "queryId2"
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
        }).catch(function (error) { fail(error); });
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
        }).catch(function (error) { fail(error); });
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
        }).catch(function (error) { fail(error); });
      });
     
      it("addQube calls remote to add qube and fires analysis:qubeAdded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "createQube") {
            expect(commandData).toEqual(createQubeCommand);
            res.data("qube", testQubeResult);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventQube = undefined;
        rootScope.$on("analysis:qubeAdded", function(event, qube) {
          eventQube = qube;
        });
        
        analysisService.setLoadedAnalysis(testEmptyAnalysis.analysis);
        var addPromise = analysisService.addQube("qubeName", "sliceId");
        
        addPromise.then(function(qube) {
          expect(qube).toEqual(testQubeResult.qube);
          
          // loadedAnalysis should contain new qube
          expect(analysisService.loadedAnalysis.qubes.filter(function(qube) {
            return qube.id === testQubeResult.qube.id;
          })[0]).toEqual(testQubeResult.qube);
          
          expect(eventQube).toEqual(testQubeResult.qube);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("addSlice calls remote to add slice and fires analysis:sliceAdded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "createSlice") {
            expect(commandData).toEqual(createSliceCommand);
            res.data("slice", testSliceResult);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventSlice = undefined;
        rootScope.$on("analysis:sliceAdded", function(event, slice) {
          eventSlice = slice;
        });
        
        analysisService.setLoadedAnalysis(testEmptyAnalysis.analysis);
        var addPromise = analysisService.addSlice("sliceName");
        
        addPromise.then(function(slice) {
          expect(slice).toEqual(testSliceResult.slice);
          
          // loadedAnalysis should contain new slice
          expect(analysisService.loadedAnalysis.slices.filter(function(slice) {
            return slice.id === testSliceResult.slice.id;
          })[0]).toEqual(testSliceResult.slice);
          
          expect(eventSlice).toEqual(testSliceResult.slice);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("addQuery calls remote to add query and fires analysis:queryAdded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "createQuery") {
            expect(commandData).toEqual(createQueryCommand);
            res.data("query", testQueryResult);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventQuery = undefined;
        rootScope.$on("analysis:queryAdded", function(event, query) {
          eventQuery = query;
        });
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis.analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id == "qubeId1"; })[0];
        
        var addPromise = analysisService.addQuery("queryName", "queryDiql", targetQube.id);
        
        addPromise.then(function(query) {
          expect(query).toEqual(testQueryResult.query);

          // target qube should contain new query
          expect(targetQube.queries.filter(function(query) {
            return query.id === testQueryResult.query.id;
          })[0]).toEqual(testQueryResult.query);
          
          expect(eventQuery).toEqual({ qubeId: targetQube.id, query: testQueryResult.query } );
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      
      it("provideQueryResults calls remote and calls intermediate function correctly", function(testDone) {
        var resultFn = undefined;
        
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysisQuery") {
            expect(commandData).toEqual(analysisQueryCommand);
            resultFn = res;
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis.analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        var curIntermediateResult = undefined;
        var intermediateResultFn = function (intermediateResult) {
          curIntermediateResult = intermediateResult;
        }
        targetQuery.results = undefined; // ensure that not another test put results there already.

        var queryPromise = analysisService.provideQueryResults(targetQube, targetQuery, intermediateResultFn);
        
        expect(resultFn).not.toBe(undefined);
        
        resultFn.data("table", testTableResult1);
        expect(curIntermediateResult).toEqual({
          exception: undefined,
          columnNames: ["colA", "colB"],
          rows: [
            [ 1, 1 ],
            [101, 101]
                 ],
          percentComplete: 50
        });
        expect(curIntermediateResult).toBe(targetQuery.results);
        
        resultFn.data("table", testTableResult2);
        expect(curIntermediateResult).toEqual({
          exception: undefined,
          columnNames: ["colA", "colB"],
          rows: [
            [ 2, 2 ],
            [102, 102]
                 ],
          percentComplete: 99
        });
        expect(curIntermediateResult).toBe(targetQuery.results);
        
        resultFn.done();
        
        queryPromise.then(function(result) {
          expect(result).toEqual({
            exception: undefined,
            columnNames: ["colA", "colB"],
            rows: [
              [ 2, 2 ],
              [102, 102]
                   ],
            percentComplete: 100
          });
          expect(result).toBe(targetQuery.results);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("provideQueryResults does not call remote for calcualted results", function(testDone) {
        var remoteServiceWasCalled = false;
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysisQuery") {
            remoteServiceWasCalled = true;
            res.data("table", testTableResult2);
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis.analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        targetQuery.results = undefined; // ensure that not another test put results there already.
        
        var queryPromise = analysisService.provideQueryResults(targetQube, targetQuery);
        
        queryPromise.then(function(result) {
          expect(result).not.toBe(undefined);
          
          expect(remoteServiceWasCalled).toBe(true);
          remoteServiceWasCalled = false;
          
          // re-run
          queryPromise = analysisService.provideQueryResults(targetQube, targetQuery);
          
          queryPromise.then(function(result) {
            expect(result).not.toBe(undefined);
            
            expect(remoteServiceWasCalled).toBe(false);
            
            testDone();
          }).catch(function(error) {
            fail(error);
          })
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("provideQueryResults handles failures correclty", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysisQuery") {
            res.exception("expectedException");
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis.analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        targetQuery.results = undefined; // ensure that not another test put results there already.
        
        var queryPromise = analysisService.provideQueryResults(targetQube, targetQuery);
        
        queryPromise.then(function(result) {
          fail("Did expect to get an exception, but succeeded?!");
        }).catch(function (result) {
          expect(result.exception).toEqual("expectedException");
          testDone();
        });
      });
    });
  });
})();