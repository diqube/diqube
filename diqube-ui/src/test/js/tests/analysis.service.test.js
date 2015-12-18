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
  
  var testEmptyAnalysis =  function() { 
    return {
      analysis: {
        id: "analysisId",
        table: "analysisTable",
        name: "analysisName",
        user: "userName",
        version: 0,
        qubes: [],
        slices: []
      }
    };
  }
  validatedData.data("analysis", testEmptyAnalysis());
  
  var testTwoQubeAnalysis = function() {
    return {
      analysis: {
        id: "analysisId",
        table: "analysisTable",
        name: "analysisName",
        user: "userName",
        version: 0,
        qubes: [ {
          id: "qubeId1",
          name: "qubeName1",
          sliceId: "sliceId1",
          queries: []
        }, {
          id: "qubeId2",
          name: "qubeName2",
          sliceId: "sliceId2",
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
        },
        {
          id: "sliceId2",
          name: "sliceName2",
          sliceDisjunctions: [],
          manualConjunction: ""
        }]
      }
    };
  };
  validatedData.data("analysis", testTwoQubeAnalysis());
  
  var testQubeResult = function() {
    return {
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
      };
  };
  validatedData.data("qube", testQubeResult());
  
  var testQubeResultUpdated = function() {
    return {
      qube: {
        id: "qubeId1",
        sliceId: "newSliceId",
        name: "newQubeName",
        queries: [
          {
            id: "queryId",
            name: "queryName",
            diql: "queryDiql",
            displayType: "table"
          }]
      }
    };
  };
  validatedData.data("qube", testQubeResultUpdated());
  
  var testSliceResult = function () {
    return {
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
    };
  };
  validatedData.data("slice", testSliceResult());
  
  var testSliceResultUpdated = function() {
    return {
      slice: {
        id: "sliceId2",
        name: "sliceNewName",
        sliceDisjunctions: [ ],
        manualConjunction: "newmanualconjunction"
      }
    };
  };
  validatedData.data("slice", testSliceResultUpdated());
  
  var testQueryResult =function () {
    return {
      query: {
        id: "queryId",
        name: "queryName",
        diql: "queryDiql",
        displayType: "table"
      }
    };
  };
  validatedData.data("query", testQueryResult());
  
  var testQueryResultAfterUpdate = function() {
    return {
      query: {
        id: "queryId2",
        name: "queryNameNew",
        diql: "queryDiqlNew",
        displayType: "tableNew"
      }
    };
  };
  validatedData.data("query", testQueryResultAfterUpdate());
  
  var testTableResult1 = function() { 
    return {
      columnNames: [ "colA", "colB" ],
      rows: [
        [ 1, 1 ],
        [ 101, 101 ]
             ],
      percentComplete: 50
    };
  };
  validatedData.data("table", testTableResult1());
  
  var testTableResult2 = function () {
    return {
      columnNames: [ "colA", "colB" ],
      rows: [
        [ 2, 2 ],
        [ 102, 102 ]
             ],
      percentComplete: 99
    };
  };
  validatedData.data("table", testTableResult2());
  
  var analysisLoadCommand = function () {
    return {
      analysisId: "analysisId",
      analysisVersion: 0
    };
  };
  validatedData.commandData("analysis", analysisLoadCommand());
  
  var createQubeCommand = function () {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      name: "qubeName", 
      sliceId: "sliceId"
    };
  };
  validatedData.commandData("createQube", createQubeCommand());
  
  var createSliceCommand = function() {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      name: "sliceName",
      manualConjunction: "",
      sliceDisjunctions: []
    };
  };
  validatedData.commandData("createSlice", createSliceCommand());
  
  var createQueryCommand = function() {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      qubeId: "qubeId1",
      name: "queryName",
      diql: "queryDiql"
    };
  };
  validatedData.commandData("createQuery", createQueryCommand());
  
  var analysisQueryCommand = function() {
    return {
      analysisId: "analysisId", 
      analysisVersion: 0,
      qubeId: "qubeId2",
      queryId: "queryId2"
    };
  };
  validatedData.commandData("analysisQuery", analysisQueryCommand());
  
  var updateQueryCommand = function() {
    return {
      analysisId: "analysisId", 
      analysisVersion: 0,
      qubeId: "qubeId2",
      newQuery: {
        id: "queryId2",
        name: "queryNameNew",
        diql: "queryDiqlNew",
        displayType: "tableNew"
      }
    };
  };
  validatedData.commandData("updateQuery", updateQueryCommand());
  
  var updateSliceCommand = function() {
    return {
      analysisId: "analysisId", 
      analysisVersion: 0,
      slice: {
        id: "sliceId2",
        name: "sliceNewName",
        sliceDisjunctions: [ ],
        manualConjunction: "newmanualconjunction"
      }
    };
  };
  validatedData.commandData("updateSlice", updateSliceCommand());
  
  var updateQubeCommand = function() {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      qubeId: "qubeId1",
      qubeName: "newQubeName",
      sliceId: "newSliceId"
    };
  };
  validatedData.commandData("updateQube", updateQubeCommand());
  
  var removeQueryCommand = function() {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      qubeId: "qubeId2",
      queryId: "queryId2"
    };
  };
  validatedData.commandData("removeQuery", removeQueryCommand());
  
  var removeQubeCommand = function() {
    return {
      analysisId: "analysisId",
      analysisVersion: 0,
      qubeId: "qubeId2"
    };
  };
  validatedData.commandData("removeQube", removeQubeCommand());
  
  var removeSliceCommand = function() {
    return {
      analysisId: "analysisId", 
      analysisVersion: 0,
      sliceId: "sliceId2"
    };
  };
  validatedData.commandData("removeSlice", removeSliceCommand());
  
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
    
    var $controller, $location, $timeout;
    beforeEach(inject(function(_$controller_, _$location_, _$timeout_){
      $controller = _$controller_;
      $location = _$location_;
      $timeout = _$timeout_;
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
            expect(commandData).toEqual(analysisLoadCommand());
            res.data("analysis", testEmptyAnalysis());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventAnalysis = undefined;
        rootScope.$on("analysis:loaded", function(event, analysis) {
          eventAnalysis = analysis;
        });
        
        var loadPromise = analysisService.loadAnalysis("analysisId", 0);
        
        loadPromise.then(function(analysis) {
          expect(analysis).toEqual(testEmptyAnalysis().analysis);
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
            res.data("analysis", testEmptyAnalysis());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var loadPromise = analysisService.loadAnalysis("analysisId", 0);
        
        loadPromise.then(function(firstAnalysis) {
          expect(numberOfAnalysisRequestedFromRemote).toBe(1);
          
          var eventAnalysis = undefined;
          rootScope.$on("analysis:loaded", function(event, analysis) {
            eventAnalysis = analysis;
          });
          
          // re-load
          loadPromise = analysisService.loadAnalysis("analysisId", 0);
          loadPromise.then(function(analysis) {
            expect(analysis).toEqual(testEmptyAnalysis().analysis);
            expect(analysisService.loadedAnalysis).toBe(analysis);
            
            expect(eventAnalysis).not.toBe(undefined);
            expect(eventAnalysis).toBe(analysis);
            
            // still "1" as remoteService should not have been called again!
            expect(numberOfAnalysisRequestedFromRemote).toBe(1); 
            testDone();
          }).catch(function (error) {
            fail(error);
          });
          
          $timeout.flush();
        }).catch(function (error) { fail(error); });
      });
      
      
      it("unloadAnalysis unloads analysis and fires analysis:loaded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysis") {
            res.data("analysis", testEmptyAnalysis());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var loadPromise = analysisService.loadAnalysis("analysisId", 0);
        
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
            expect(commandData).toEqual(createQubeCommand());
            res.data("qube", testQubeResult());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventQube = undefined;
        rootScope.$on("analysis:qubeAdded", function(event, qube) {
          eventQube = qube;
        });
        
        analysisService.setLoadedAnalysis(testEmptyAnalysis().analysis);
        var addPromise = analysisService.addQube("qubeName", "sliceId");
        
        addPromise.then(function(qube) {
          expect(qube).toEqual(testQubeResult().qube);
          
          // loadedAnalysis should contain new qube
          expect(analysisService.loadedAnalysis.qubes.filter(function(qube) {
            return qube.id === testQubeResult().qube.id;
          })[0]).toEqual(testQubeResult().qube);
          
          expect(eventQube).toEqual(testQubeResult().qube);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("addSlice calls remote to add slice and fires analysis:sliceAdded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "createSlice") {
            expect(commandData).toEqual(createSliceCommand());
            res.data("slice", testSliceResult());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventSlice = undefined;
        rootScope.$on("analysis:sliceAdded", function(event, slice) {
          eventSlice = slice;
        });
        
        analysisService.setLoadedAnalysis(testEmptyAnalysis().analysis);
        var addPromise = analysisService.addSlice("sliceName", "", []);
        
        addPromise.then(function(slice) {
          expect(slice).toEqual(testSliceResult().slice);
          
          // loadedAnalysis should contain new slice
          expect(analysisService.loadedAnalysis.slices.filter(function(slice) {
            return slice.id === testSliceResult().slice.id;
          })[0]).toEqual(testSliceResult().slice);
          
          expect(eventSlice).toEqual(testSliceResult().slice);
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      it("addQuery calls remote to add query and fires analysis:queryAdded event", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "createQuery") {
            expect(commandData).toEqual(createQueryCommand());
            res.data("query", testQueryResult());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var eventQuery = undefined;
        rootScope.$on("analysis:queryAdded", function(event, query) {
          eventQuery = query;
        });
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis().analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id == "qubeId1"; })[0];
        
        var addPromise = analysisService.addQuery("queryName", "queryDiql", targetQube.id);
        
        addPromise.then(function(query) {
          expect(query).toEqual(testQueryResult().query);

          // target qube should contain new query
          expect(targetQube.queries.filter(function(query) {
            return query.id === testQueryResult().query.id;
          })[0]).toEqual(testQueryResult().query);
          
          expect(eventQuery).toEqual({ qubeId: targetQube.id, query: testQueryResult().query } );
          testDone();
        }).catch(function (error) {
          fail(error);
        });
      });
      
      
      it("provideQueryResults calls remote and calls intermediate function correctly", function(testDone) {
        var resultFn = undefined;
        
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "analysisQuery") {
            expect(commandData).toEqual(analysisQueryCommand());
            resultFn = res;
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis().analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        var curIntermediateResult = undefined;
        var intermediateResultFn = function (intermediateResult) {
          curIntermediateResult = intermediateResult;
        }
        targetQuery.$results = undefined; // ensure that not another test put results there already.

        var queryPromise = analysisService.provideQueryResults(targetQube, targetQuery, intermediateResultFn);
        
        expect(resultFn).not.toBe(undefined);
        
        resultFn.data("table", testTableResult1());
        expect(curIntermediateResult).toEqual({
          exception: undefined,
          columnNames: ["colA", "colB"],
          rows: [
            [ 1, 1 ],
            [101, 101]
                 ],
          percentComplete: 50
        });
        expect(curIntermediateResult).toBe(targetQuery.$results);
        
        resultFn.data("table", testTableResult2());
        expect(curIntermediateResult).toEqual({
          exception: undefined,
          columnNames: ["colA", "colB"],
          rows: [
            [ 2, 2 ],
            [102, 102]
                 ],
          percentComplete: 99
        });
        expect(curIntermediateResult).toBe(targetQuery.$results);
        
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
          expect(result).toBe(targetQuery.$results);
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
            res.data("table", testTableResult2());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis().analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        targetQuery.$results = undefined; // ensure that not another test put results there already.
        
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
        
        analysisService.setLoadedAnalysis(testTwoQubeAnalysis().analysis);
        var targetQube = analysisService.loadedAnalysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        targetQuery.$results = undefined; // ensure that not another test put results there already.
        
        var queryPromise = analysisService.provideQueryResults(targetQube, targetQuery);
        
        queryPromise.then(function(result) {
          fail("Did expect to get an exception, but succeeded?!");
        }).catch(function (result) {
          expect(result.exception).toEqual("expectedException");
          testDone();
        });
      });
      
      it("updateQuery sends updates to server", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "updateQuery") {
            expect(commandData).toEqual(updateQueryCommand());
            res.data("query", testQueryResultAfterUpdate());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = angular.copy(testTwoQubeAnalysis().analysis);
        analysisService.setLoadedAnalysis(analysis);
        var targetQube = analysis.qubes.filter(function(qube) { return qube.id === "qubeId2"; })[0];
        var targetQuery = targetQube.queries.filter(function(query) { return query.id === "queryId2"; })[0];
        
        var updatePromise = analysisService.updateQuery(targetQube.id, updateQueryCommand().newQuery);
        
        updatePromise.then(function(result) {
          expect(result).toEqual(updateQueryCommand().newQuery);
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
      
      it("updateQube sends updates to server", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "updateQube") {
            expect(commandData).toEqual(updateQubeCommand());
            res.data("qube", testQubeResultUpdated());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = angular.copy(testTwoQubeAnalysis().analysis);
        analysisService.setLoadedAnalysis(analysis);
        
        var updatePromise = analysisService.updateQube(testQubeResultUpdated().qube);
        
        updatePromise.then(function() {
          var targetQube = analysis.qubes.filter(function(qube) { return qube.id === "qubeId1"; })[0];
          expect(targetQube.name).toEqual("newQubeName");
          expect(targetQube.sliceId).toEqual("newSliceId");
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
      
      it("updateSlice sends updates to server and clears results", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "updateSlice") {
            expect(commandData).toEqual(updateSliceCommand());
            res.data("slice", testSliceResultUpdated());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = testTwoQubeAnalysis().analysis;
        analysisService.setLoadedAnalysis(analysis);
        
        // add "results" to a query that will be affected by the slice update!
        var query = analysis.qubes.filter(function(q) {  return q.sliceId === "sliceId2"; })[0].queries[0];
        query.$results = { };
        
        var updatePromise = analysisService.updateSlice(testSliceResultUpdated().slice);
        
        updatePromise.then(function() {
          var slice = analysis.slices.filter(function(slice) { return slice.id === updateSliceCommand().slice.id; })[0];
          expect(slice).toEqual(updateSliceCommand().slice);
          
          expect(query.$results).toBe(undefined);
          
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
      
      it("removeQuery sends remove to server and clears local objects", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "removeQuery") {
            expect(commandData).toEqual(removeQueryCommand());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = angular.copy(testTwoQubeAnalysis().analysis);
        analysisService.setLoadedAnalysis(analysis);

        var testQube = analysis.qubes.filter(function(q) { return q.id === "qubeId2"; })[0];
        
        var removePromise = analysisService.removeQuery(testQube.id, "queryId2");
        
        removePromise.then(function() {
          expect(testQube.queries.length).toEqual(0);
          
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
      
      it("removeQube sends remove to server and clears local objects", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "removeQube") {
            expect(commandData).toEqual(removeQubeCommand());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = angular.copy(testTwoQubeAnalysis().analysis);
        analysisService.setLoadedAnalysis(analysis);

        var removePromise = analysisService.removeQube("qubeId2");
        
        removePromise.then(function() {
          expect(analysis.qubes.filter(function(q) { return q.id === "qubeId2"; }).length).toEqual(0);
          
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
      
      it("removeSlice sends remove to server and clears local objects", function(testDone) {
        remoteServiceHandlerFn = function(res, commandName, commandData) {
          if (commandName === "removeSlice") {
            expect(commandData).toEqual(removeSliceCommand());
            res.done();
          } else
            fail("Unexpected command sent by analysisService: " + commandName + ", " + commandData);
        }
        
        var analysis = angular.copy(testTwoQubeAnalysis().analysis);
        analysisService.setLoadedAnalysis(analysis);

        var removePromise = analysisService.removeSlice("sliceId2");
        
        removePromise.then(function() {
          expect(analysis.slices.filter(function(s) { return s.id === "sliceId2"; }).length).toEqual(0);
          
          testDone();
        }).catch(function (text) {
          fail(text);
        });
      });
    });
  });
})();