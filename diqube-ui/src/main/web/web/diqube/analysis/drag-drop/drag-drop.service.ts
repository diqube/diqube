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

import {Injectable} from "angular2/core";
import * as dragData from "./drag-drop.data";

export interface DragDropListener {
  dragStarted(element: dragData.DragDropElement): void;
  dragEnded(): void;
}

@Injectable()
export class DragDropService {
  public currentDrag: dragData.DragDropElement;
  private listeners: Array<DragDropListener> = [];
  
  public startDrag(element: dragData.DragDropElement): void {
    console.log("Starting to drag", element);
    this.currentDrag = element;
    
    this.listeners.forEach((l: DragDropListener) => { l.dragStarted(element); });
  }
  
  public startDragRestriction(field: string, value: string): void {
    // TODO #48: Use definitive information from server on what data type this value should have!
    // For now we use a heuristic that checks the data type of the JS object
    if (typeof value === "string")
      value = "'" + value + "'";
    
    var data: dragData.DragDropRestrictionData = new dragData.DragDropRestrictionData(field, value);
    var dragEl: dragData.DragDropElement = new dragData.DragDropElement(dragData.DragDropRestrictionData.TYPE, data);
    
    this.startDrag(dragEl);
  }
  
  public stopDrag(): void {
    console.log("Finished drag");
    this.currentDrag = undefined;
    
    this.listeners.forEach((l: DragDropListener) => { l.dragEnded(); });
  }
  
  public addListener(listener: DragDropListener): void {
    this.listeners.push(listener);
  }
  
  public removeListener(listener: DragDropListener): void {
    this.listeners.splice(this.listeners.indexOf(listener), 1);
  }
}
