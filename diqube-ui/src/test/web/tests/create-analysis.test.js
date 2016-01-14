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

  var testTableNameList = validatedData.data("tableNameList", {
    tableNames: [ "table1", "table2" ]
  });
  
  var testEmptyAnalysis = validatedData.data("analysis", {
    analysis: {
      id: "analysisId",
      table: "analysisTable",
      name: "analysisName",
      user: "userName",
      version: 0,
      qubes: [],
      slices: []
    }
  });
  
  var createAnalysisCommandData = validatedData.commandData("createAnalysis", {
    name: "abc", 
    table: "xyz"
  });
  
  describe("diqube.create-analysis module", function() {
    beforeEach(module("diqube.create-analysis"));
    var $controller, $location;
    beforeEach(inject(function(_$controller_, _$location_){
      $controller = _$controller_;
      $location = _$location_;
    }));
    
    describe("CreateAnalysisCtrl", function() {
      var $scope, controller, mockedLoginStateService;
      beforeEach(function() {
        $scope = new MockedScope();
        mockedLoginStateService = new MockedLoginStateService();
      });

      it("tables are loaded", function(testDone) {
        inject(function($controller) {
          var controller = $controller("CreateAnalysisCtrl", { 
            $scope: $scope,
            // a remoteService which simply returns the test data.
            remoteService: new MockedRemoteService(function(res) { res.data("tableNameList", testTableNameList); res.done(); }),
            loginStateService: mockedLoginStateService
          });
          
          var doneCounterCount = 0;
          var doneCounter = function() {
            doneCounterCount++;
            if (doneCounterCount == 3)
              testDone();
          }
          
          controller.getValidTables("table").then(function(tables) {
            expect(tables).toEqual([ { name: "table1" }, { name: "table2" } ]);
            doneCounter();
          }).catch(function(text) { fail(text) } );
          controller.getValidTables("").then(function(tables) {
            expect(tables).toEqual([ { name: "table1" }, { name: "table2" } ]);
            doneCounter();
          }).catch(function(text) { fail(text) } );
          controller.getValidTables("1").then(function(tables) {
            expect(tables).toEqual([ { name: "table1" } ]);
            doneCounter();
          }).catch(function(text) { fail(text) } );
          controller.getValidTables("x").then(function(tables) {
            expect(tables).toEqual([ ]);
            doneCounter();
          }).catch(function(text) { fail(text) } );
        });
      });
      
      it("createAnalysis needs name", inject(function($controller) {
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          remoteService: new MockedRemoteService(function(res) { res.done(); }),
          loginStateService: mockedLoginStateService
        });
        
        controller.createAnalysis({ table: "xyz" });
        
        expect(controller.error).not.toBe(undefined);
      }));
      
      it("createAnalysis needs table", inject(function($controller) {
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          remoteService: new MockedRemoteService(function(res) { res.done(); }),
          loginStateService: mockedLoginStateService
        });
        
        controller.createAnalysis({ name: "xyz" });
        
        expect(controller.error).not.toBe(undefined);
      }));
      
      it("createAnalysis needs name", inject(function($controller) {
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          remoteService: new MockedRemoteService(function(res) { res.done(); }),
          loginStateService: mockedLoginStateService
        });
        
        controller.createAnalysis({ table: "xyz" });
        
        expect(controller.error).not.toBe(undefined);
      }));

      it("createAnalysis sends create command to server with valid params", inject(function($controller) {
        var commandIssued = undefined;
        var commandDataSent = undefined;
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          remoteService: new MockedRemoteService(function(res, commandName, commandData) {
            commandIssued = commandName;
            commandDataSent = commandData;
            res.data("analysis", testEmptyAnalysis);
            res.done(); 
          }),
          loginStateService: mockedLoginStateService
        });
        
        controller.createAnalysis({ name: "abc", table: "xyz" });
        
        expect(controller.error).toBe(undefined);
        expect(commandIssued).toEqual("createAnalysis");
        expect(commandDataSent).toEqual(createAnalysisCommandData);
      }));
      
      it("createAnalysis redirects", inject(function($controller) {
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          remoteService: new MockedRemoteService(function(res) { res.data("analysis", testEmptyAnalysis); res.done(); }),
          loginStateService: mockedLoginStateService
        });
        
        spyOn($location, "path");
        
        controller.createAnalysis({ name: "abc", table: "xyz" });
        
        expect(controller.error).toBe(undefined);
        expect($location.path).toHaveBeenCalledWith("analysis/analysisId");
      }));
      
      it("createAnalysis boradcasts analysis:created event", inject(function($controller) {
        var commandIssued = undefined;
        var controller = $controller("CreateAnalysisCtrl", { 
          $scope: $scope,
          $rootScope: $scope,
          remoteService: new MockedRemoteService(function(res) { res.data("analysis", testEmptyAnalysis); res.done(); }),
          loginStateService: mockedLoginStateService
        });
        
        var eventDataBroadcasted = undefined;
        
        $scope.$on("analysis:created", function (event, data) {
          eventDataBroadcasted = data;
        });
        
        controller.createAnalysis({ name: "abc", table: "xyz" });
        
        expect(controller.error).toBe(undefined);
        expect(eventDataBroadcasted).not.toBe(undefined);
      }));
      
    });
  });
})();