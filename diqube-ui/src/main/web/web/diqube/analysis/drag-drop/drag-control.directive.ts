///
/// diqube: Distributed Query Base.
///
/// Copyright (C) 2015 Bastian Gloeckle
///
/// This file is part of diqube.
///
/// diqube is free software: you can redistribute it and/or modify
/// it under the terms of the GNU Affero General Public License as
/// published by the Free Software Foundation, either version 3 of the
/// License, or (at your option) any later version.
///
/// This program is distributed in the hope that it will be useful,
/// but WITHOUT ANY WARRANTY; without even the implied warranty of
/// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/// GNU Affero General Public License for more details.
///
/// You should have received a copy of the GNU Affero General Public License
/// along with this program.  If not, see <http://www.gnu.org/licenses/>.
///

import {Directive, Input, OnInit, OnDestroy} from "angular2/core";
import {TemplateRef, ViewContainerRef, ElementRef} from "angular2/core";
import {DragDropService, DragDropListener} from "./drag-drop.service";
import * as dragData from "./drag-drop.data";

/**
 * Directive taking care that a hovered element is displayed during a drag operation. The value set to the directive is
 * the ID of a HTML element that will be used to moved around when dragging (just use an empty <span>, set display: none).
 */
@Directive({ 
  selector: "[dragControl]"
})
export class DragControlDirective implements OnInit, OnDestroy, DragDropListener {
  private dragControlElement: HTMLElement = undefined;  
  
  private mouseMoveHandler: (event: MouseEvent) => void;
  private mouseUpHandler: (event: MouseEvent) => void;
  
  constructor(private parentElement: ElementRef, private dragDropService: DragDropService) {}
  
  @Input() set dragControl(elementId: string) {
    this.dragControlElement = document.getElementById(elementId);
  }
  
  public ngOnInit(): any {
    this.dragDropService.addListener(this);
    
    this.mouseMoveHandler = (event: MouseEvent) => {
      if (this.dragDropService.currentDrag !== undefined) {
        this.dragControlElement.style.display = "inline";

        var width = this.dragControlElement.getBoundingClientRect().width;
        var height = this.dragControlElement.getBoundingClientRect().height;
        var mouseX = event.pageX;
        var mouseY = event.pageY;
        
        var targetX = mouseX - width - 1;
        var targetY = mouseY + 1;
        
        if (targetX < 5)
          targetX = mouseX + 2;
        if (mouseY + height > document.body.clientHeight - 5)
          mouseY = mouseY - height - 2;
        
        this.dragControlElement.style.zIndex = "200";
        this.dragControlElement.style.position = "absolute";
        this.dragControlElement.style.left = targetX + "px";
        this.dragControlElement.style.top = targetY + "px";
      }
    };
    
    this.mouseUpHandler = (event: MouseEvent) => {
      if (this.dragDropService.currentDrag !== undefined) {
        this.dragDropService.stopDrag();
        event.preventDefault();
        event.stopPropagation();
      }
    };
    
    this.parentElement.nativeElement.addEventListener("mousemove", this.mouseMoveHandler);
    this.parentElement.nativeElement.addEventListener("mouseup", this.mouseUpHandler);
  }
  
  public ngOnDestroy(): any {
    this.dragDropService.removeListener(this);
    
    this.parentElement.nativeElement.removeEventListener("mousemove", this.mouseMoveHandler);
    this.parentElement.nativeElement.removeEventListener("mouseup", this.mouseUpHandler);
  }
  
  public dragStarted(element: dragData.DragDropElement): void {
    if (!this.dragControlElement)
      return;

    if (element.type === dragData.DragDropRestrictionData.TYPE)
      this.dragControlElement.textContent = (<dragData.DragDropRestrictionData>element.data).value;
    else
      this.dragControlElement.textContent = " ";
  }
  
  public dragEnded(): void {
    if (!this.dragControlElement)
      return;

    this.dragControlElement.style.display = "none";
  }

}