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
  angular.module("diqube.drag-drop").service("dragDropService",
      [ "$log", "$rootScope", function dragDropProvider($log, $rootScope) {
        var me = this;
        
        me.currentDrag = undefined;
        me.startDrag = startDrag;
        me.startDragRestriction = startDragRestriction;
        me.stopDrag = stopDrag;
        
        // ====
        
        function startDrag(dragDropElement) {
          $log.info("Starting to drag", dragDropElement);
          me.currentDrag = dragDropElement;
          $rootScope.$broadcast("drag:started", dragDropElement);
        }
        
        function startDragRestriction(field, value) {
          var dndEl = new DragDropElement();
          dndEl.type = DragDropElement.TYPE_RESTRICTION;
          dndEl.data = new DragDropRestrictionData();
          dndEl.data.field = field;
          
          // TODO #48: Use definitive information from server on what data type this value should have!
          // For now we use a heuristic that checks the data type of the JS object
          if (typeof value === "string")
            dndEl.data.value = "'" + value + "'";
          else
            dndEl.data.value = value;
          
          startDrag(dndEl);
        }
        
        function stopDrag() {
          $log.info("Finished drag");
          me.currentDrag = undefined;
          $rootScope.$broadcast("drag:ended");
        }
      } ]);
})();