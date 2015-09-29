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

  var versionData = validatedData.data("version", {
    gitCommitLong: "abcdef",
    gitCommitShort: "abc",
    buildTimestamp: "today"
  });
  
  
  describe("diqube.about module", function() {
    beforeEach(module("diqube.about"));
    var $controller;
    beforeEach(inject(function(_$controller_){
      $controller = _$controller_;
    }));
    
    
    describe("AboutCtrl", function() {
      var $scope, aboutCtrl;
      beforeEach(function() {
        $scope = { $apply : function(fn) { fn(); }};
        aboutCtrl = $controller("AboutCtrl", { 
          $scope: $scope,
          // a remoteService which simply returns the versionData object.
          remoteService: new MockedRemoteService(function(res) { res.data("version", versionData); res.done(); })
        });
      });

      it('should have correct data', inject(function($controller) {
        expect(aboutCtrl).toBeDefined();
        expect(aboutCtrl.gitcommit).toBe("abc");
        expect(aboutCtrl.gitcommitlong).toBe("abcdef");
        expect(aboutCtrl.buildtimestamp).toBe("today");
      }));
    });
  });
})();