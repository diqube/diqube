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

import {Component, Input, OnInit, Output, EventEmitter} from "angular2/core";
import {DiqubeUtil} from "../util/diqube.util";

@Component({
  selector: "diqube-table-order",
  templateUrl: "diqube/table/table.order.html"
})
export class DiqubeTableOrderComponent {
  public static STATE_NONE: string = "none";
  public static STATE_ASC: string = "asc";
  public static STATE_DESC: string = "desc";    
  
  @Input("state") public state: string = DiqubeTableOrderComponent.STATE_NONE;
  
  @Output("onChange") public onChange: EventEmitter<string> = new EventEmitter<string>();
  
  @Input("isHovered") public isHovered: boolean;
  
  public onClick(event: MouseEvent, clickTargetState: string): void {
    if (this.state === clickTargetState)
      return;
    
    this.onChange.emit(clickTargetState);
  }
}