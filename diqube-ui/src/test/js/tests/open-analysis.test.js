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

  var testData1 = validatedData.data("analysisRef", {
    name: "testName",
    id: "testId",
  });
  
  var testData2 = validatedData.data("analysisRef", {
    name: "testName2",
    id: "testId2",
  });
  
  
  describe("diqube.open-analysis module", function() {
    beforeEach(module("diqube.open-analysis"));
    var $controller, $location;
    beforeEach(inject(function(_$controller_, _$location_){
      $controller = _$controller_;
      $location = _$location_;
    }));
    
    
    describe("OpenAnalysisCtrl", function() {
      var $scope, controller;
      beforeEach(function() {
        $scope = new MockedScope();
      });

      it("should have correct data when initially loaded", inject(function($controller) {
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { res.data("analysisRef", testData1); res.done(); })
        });
        
        expect(controller.items).toEqual([ { name: "testName", id: "testId" } ]);
      }));
      
      it("should have correct data when re-loaded", inject(function($controller) {
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1); res.done();
        }
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });
        
        mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1);
          res.data("analysisRef", testData2);
          res.done();
        }
        
        controller.reloadAnalysis();
        
        expect(controller.items).toEqual([ { name: "testName", id: "testId" }, { name: "testName2", id: "testId2" } ]);
      }));
      
      it("listens to analysis:created", inject(function($controller) {
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1); res.done();
        }
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });
        
        mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1);
          res.data("analysisRef", testData2);
          res.done();
        }
        
        $scope.$broadcast("analysis:created");
        
        expect(controller.items).toEqual([ { name: "testName", id: "testId" }, { name: "testName2", id: "testId2" } ]);
      }));
      
      it("listens to analysis:loaded", inject(function($controller) {
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1); res.done();
        }
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });
        
        
        $scope.$broadcast("analysis:loaded", testData1);
        
        expect(controller.text).toEqual("testName");
        expect(controller.title).toEqual("testId");
      }));
      
      it("listens to analysis:loaded with undefined analysis", inject(function($controller) {
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1); res.done();
        }
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });
        
        $scope.$broadcast("analysis:loaded", undefined);
        
        expect(controller.text).not.toEqual("testName");
        expect(controller.title).not.toEqual("testId");
      }));
      
      it("open analysis actually opens", inject(function($controller) {
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1); res.done();
        }
        var controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });

        spyOn($location, 'path');
        
        controller.openAnalysis(testData1);
       
        expect($location.path).toHaveBeenCalledWith("analysis/testId");
      }));
      
      it("loading flag is corrct", inject(function($controller) {
        var validate = false;
        var controller;
        var mockedRemoteServiceHandlerFn = function(res) {
          res.data("analysisRef", testData1);
          if (validate)
            expect(controller.loading).toEqual(true);
          res.done();
          if (validate)
            expect(controller.loading).toEqual(false);
        }
        
        controller = $controller("OpenAnalysisCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the testData1 object.
          remoteService: new MockedRemoteService(function(res) { mockedRemoteServiceHandlerFn(res); })
        });
        
        validate = true;
        controller.reloadAnalysis();
      }));
    });
  });
})();