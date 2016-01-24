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

import {Directive, Input, Output, EventEmitter, OnInit, OnDestroy, ElementRef} from "angular2/core";
import * as dragData from "./drag-drop.data";
import {DragDropService, DragDropListener} from "./drag-drop.service";

/**
 * Provides a dropped element.
 */
export interface DragElementProvider {
  element(): dragData.DragDropElement;
  /** Handler must call this method in case it handled the drop successfully. */
  handled(): void;
}

/**
 * Directive that makes the attribute-bearing element a drop target. Value of the attribute is a comma-separated list of
 * values of DragDropElement.type that are accepted.
 * 
 * When something is dropped, an event is emitted on the "drop" output. That event is a DragElementProvider, whose
 * handled() method must be called if the handler successfully handled the drop. 
 */
@Directive({
  selector: "[dropTarget]"
})
export class DropTargetDirective implements OnInit, OnDestroy, DragDropListener {
  @Output("drop") drop: EventEmitter<DragElementProvider> = new EventEmitter<DragElementProvider>(false);
  
  private allDropAccept: Array<string>;
  private mouseUpHandler: (event: MouseEvent)=>void;
  
  constructor(private element: ElementRef, private dragDropService: DragDropService) {}
  
  public ngOnInit(): any {
    this.mouseUpHandler = (event: MouseEvent) => {
      if (this.dragDropService.currentDrag !== undefined) {
        if (this.accept(this.dragDropService.currentDrag)) {
          var element = this.dragDropService.currentDrag;
          var handled = false;
          this.drop.emit({
            element: () => { return element; },
            handled: () => { handled = true; }
          });
          
          if (handled) {
            this.dragDropService.stopDrag();
            event.stopPropagation();
            event.preventDefault();
          }
        }
      }
    };
    
    this.element.nativeElement.addEventListener("mouseup", this.mouseUpHandler);
    this.dragDropService.addListener(this);
  }
  
  public ngOnDestroy(): any {
    this.dragDropService.removeListener(this);
    this.element.nativeElement.removeEventListener("mouseup", this.mouseUpHandler);
  }
  
  private accept(dragElement: dragData.DragDropElement): boolean {
    return this.allDropAccept.indexOf(dragElement.type) > -1;
  }
  
  /** comma-separated list of DragDropElement.type values that are accepted */
  @Input() set dropTarget(dropAccept: string) {
    this.allDropAccept = dropAccept.split(",");
  }
  
  public dragStarted(element: dragData.DragDropElement): void {
    if (this.accept(element)) {
      if (!this.element.nativeElement.className.match(/(^|\s)droppable(?!\S)/))
        this.element.nativeElement.className += " droppable";
    }
  }
  
  public dragEnded(): void {
    this.element.nativeElement.className = this.element.nativeElement.className.replace(/(^|\s)droppable(?!\S)/g , "");
  }
}