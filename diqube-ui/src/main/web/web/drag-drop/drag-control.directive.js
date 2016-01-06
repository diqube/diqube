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
  
  angular.module("diqube.drag-drop").directive("dragControl",
      [ "$timeout", "$log", "dragDropService", function($timeout, $log, dragDropService) {
        return {
          restrict: "E",
          transclude: true,
          scope: {
          },
          templateUrl: "drag-drop/drag-control.html",
          link: function link($scope, element, attrs) {
            
            var dragControlEl = $(".diqube-drag-control");
            
            $scope.dragInProgress = false;
            
            element[0].addEventListener("mousemove", function(event) {
              if ($scope.dragInProgress) {
                var width = dragControlEl.outerWidth();
                var height = dragControlEl.outerHeight();
                var mouseX = event.pageX;
                var mouseY = event.pageY;
                
                if (mouseX + width > $(document).width() - 5)
                  mouseX = mouseX - width - 2;
                if (mouseY + height > $(document).height() - 5)
                  mouseY = mouseY - height - 2;
                
                dragControlEl.css({
                  position: "absolute",
                  left: mouseX + 1,
                  top: mouseY + 1
                });
              }
            });
            
            element[0].addEventListener("mouseup", function(event) {
              if ($scope.dragInProgress) {
                dragDropService.stopDrag();
              }
            });
            
            $scope.$on("drag:started", function(event, dragDropElement) {
              dragControlEl.removeClass("hidden");
              if (dragDropElement.type === DragDropElement.TYPE_RESTRICTION)
                dragControlEl.text(dragDropElement.data.value);
              else
                dragControlEl.text("  ");
              $scope.dragInProgress = true;
            });
            $scope.$on("drag:ended", function() {
              dragControlEl.addClass("hidden");
              $scope.dragInProgress = false;
            })
          }
        };
                
      } ]);
})();