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
  
  var testAnalysisResult = validatedData.data("analysis", {
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
  
  describe("diqube.analysis module", function() {
    describe("diqubeQuery directive", function() {
      var $compile, $scope, mockedAnalysisService, pureElement;
      
      beforeEach(function() {
        module("diqube.analysis");
        module("testHtmlTemplates");
        
        mockedAnalysisService =  new MockedAnalysisService(
            function() { return $scope; },
            function() { return testAnalysisResult.analysis; },
            function() { return { percentComplete:100, rows: [[1,2]], columnNames:["colA", "colB"] }; },
            function() { /* qube */ },
            function() { /* query */ },
            function() { /* slice */ });
        
        module(function($provide) {
          // provide mocked analysis service, this will overwrite the orig analysis service that was loaded from diqube.analysis.
          $provide.factory("analysisService", function() { 
            return mockedAnalysisService;
          });
        });
      });

      beforeEach(inject(function(_$compile_, _$rootScope_){
        $compile = _$compile_;
        $scope = _$rootScope_.$new();
        $scope.query = testAnalysisResult.analysis.qubes[1].queries[0];
        $scope.qube = testAnalysisResult.analysis.qubes[1];
        $scope.analysis = testAnalysisResult.analysis;
        
        // the values of the attributes in the following reference the fields in $scope.
        pureElement = angular.element("<diqube-query query=\"query\" qube=\"qube\" analysis=\"analysis\"></diqube-query>");
      }));

      it("does initially trigger loading data from analysisService.", function() {
        
        spyOn(mockedAnalysisService, "provideQueryResults").and.callThrough();
        
        var compiledElement = $compile(pureElement)($scope);
        $scope.$digest();

        expect(mockedAnalysisService.provideQueryResults).toHaveBeenCalled();        
      });
      
      it("switchQueryDisplayType sends updates to server", function() {
        
        spyOn(mockedAnalysisService, "updateQuery").and.callThrough();
        
        var compiledElement = $compile(pureElement)($scope);
        $scope.$digest();

        expect(mockedAnalysisService.updateQuery).not.toHaveBeenCalled();
        
        var isolatedScope = compiledElement.isolateScope();
        isolatedScope.switchQueryDisplayType("barchart");
        
        expect(mockedAnalysisService.updateQuery).toHaveBeenCalled();
        var sentQuery = mockedAnalysisService.updateQuery.calls.argsFor(0)[1];
        expect(sentQuery.displayType).toEqual("barchart");
      });
      
      it("removeQuery removes query from server", function() {
        
        spyOn(mockedAnalysisService, "removeQuery").and.callThrough();
        
        var compiledElement = $compile(pureElement)($scope);
        $scope.$digest();

        expect(mockedAnalysisService.removeQuery).not.toHaveBeenCalled();
        
        var isolatedScope = compiledElement.isolateScope();
        isolatedScope.removeQuery();
        
        expect(mockedAnalysisService.removeQuery).toHaveBeenCalled();
        var sentQueryId = mockedAnalysisService.removeQuery.calls.argsFor(0)[1];
        expect(sentQueryId).toEqual("queryId2");
      });
    });
  });
})();