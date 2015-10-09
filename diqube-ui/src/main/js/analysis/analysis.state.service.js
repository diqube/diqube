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
  
  angular.module("diqube.analysis").service("analysisStateService",
      [ 
      function () {
        var me = this;
        
        me.markToOpenQueryInEditModeNextTime = markToOpenQueryInEditModeNextTime;
        me.pollOpenQueryInEditModeNextTime = pollOpenQueryInEditModeNextTime;
        
        me.markToOpenSliceInEditModeNextTime = markToOpenSliceInEditModeNextTime;
        me.pollOpenSliceInEditModeNextTime = pollOpenSliceInEditModeNextTime;

        // ====
        
        me.$queriesToOpenInEditMode = [];
        
        function markToOpenQueryInEditModeNextTime(queryId) {
          me.$queriesToOpenInEditMode.push(queryId);
        }
        
        function pollOpenQueryInEditModeNextTime(queryId) {
          var idx = me.$queriesToOpenInEditMode.indexOf(queryId);
          if (idx === -1)
            return false;
          
          me.$queriesToOpenInEditMode.splice(idx, 1);
          return true;
        }
        
        me.$slicesToOpenInEditMode = [];
        
        function markToOpenSliceInEditModeNextTime(sliceId) {
          me.$slicesToOpenInEditMode.push(sliceId);
        }
        
        function pollOpenSliceInEditModeNextTime(sliceId) {
          var idx = me.$slicesToOpenInEditMode.indexOf(sliceId);
          if (idx === -1)
            return false;
          
          me.$slicesToOpenInEditMode.splice(idx, 1);
          return true;
        }
      }]);
})();