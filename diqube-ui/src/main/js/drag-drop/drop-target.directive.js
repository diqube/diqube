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
  
  angular.module("diqube.drag-drop").directive("dropTarget",
      [ "$timeout", "$log", "dragDropService", function($timeout, $log, dragDropService) {
        return {
          restrict: "A",
          scope: {
            accept: "@dropAccept",
            handler: "&dropHandler"
          },
          link: function link($scope, element, attrs) {
            var allAccepts = $scope.accept.split(",");
            
            var isAcceptableDragDropElement = function(elem) {
              return allAccepts.indexOf(elem.type) > -1;
            }
            
            element[0].addEventListener("mouseup", function(event) {
              if (dragDropService.currentDrag) {
                if (isAcceptableDragDropElement(dragDropService.currentDrag)) {
                  if ($scope.handler({ element : dragDropService.currentDrag })) {
                    // handler method returns true if it succeeded.
                    dragDropService.stopDrag();
                    
                    event.stopPropagation();
                    event.preventDefault();
                  }
                  // if handler returned false, let the event bubble up further, perhaps we find someone who can handle
                  // it.
                }
              }
            });
            
            $scope.$on("drag:started", function(event, dragDropElement) {
              if (isAcceptableDragDropElement(dragDropElement)) {
                element.addClass("droppable");
              }
            });
            $scope.$on("drag:ended", function() {
              element.removeClass("droppable");              
            })
          }
        };
                
      } ]);
})();