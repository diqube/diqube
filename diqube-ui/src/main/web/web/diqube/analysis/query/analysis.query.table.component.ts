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

import {Component, Input} from "angular2/core";
import * as remoteData from "../../remote/remote";
import * as analysisData from "../analysis";
import {DragDropService} from "../drag-drop/drag-drop.service";
import {DiqubeTableComponent, DiqubeTableDragStartEvent, DiqubeTableOrderedCol} from "../../table/diqube.table.component";
import {DiqubeUtil} from "../../util/diqube.util";

@Component({
  selector: "diqube-analysis-query-table",
  templateUrl: "diqube/analysis/query/analysis.query.table.html",
  directives: [ DiqubeTableComponent ]
})
export class AnalysisQueryTableComponent {
  
  @Input("query") public query: remoteData.UiQuery = undefined;
  
  @Input("queryResults") public queryResults: analysisData.EnhancedTableJsonResult = undefined;
  
  private allRowIndicesCache: Array<number> = [];
  private allColIndicesCache: Array<number> = [];
  private orderedColCache: DiqubeTableOrderedCol = undefined;
  
  constructor(private dragDropService: DragDropService) {}
  
  public allRowIndices(): Array<number> {
    var res: Array<number> = undefined;

    if (this.queryResults === undefined || this.queryResults.rows === undefined || !this.queryResults.rows.length) {
      res = [];
    } else {
      res = [];
      for (var i = 0; i < this.queryResults.rows.length; i++)
        res.push(i);
    }
    
    if (!DiqubeUtil.equals(this.allRowIndicesCache, res))
      this.allRowIndicesCache = res;
    
    return this.allRowIndicesCache;
  }
  
  public allColIndices(): Array<number> {
    var res: Array<number> = undefined;

    if (this.queryResults === undefined || this.queryResults.columnNames === undefined || !this.queryResults.columnNames.length) {
      res = [];
    } else {
      res = [];
      for (var i = 0; i < this.queryResults.columnNames.length; i++)
        res.push(i);
    }
    
    if (!DiqubeUtil.equals(this.allColIndicesCache, res))
      this.allColIndicesCache = res;
    
    return this.allColIndicesCache;
  }
  
  public onDragStart(event: DiqubeTableDragStartEvent): void {
    var colRequest: string = this.queryResults.columnRequests[0];
    var value: any = this.queryResults.rows[event.rowIdx][0];
    
    this.dragDropService.startDragRestriction(colRequest, value);
  }
  
  public orderedCol(): DiqubeTableOrderedCol {
    if (this.queryResults.queryInfo === undefined)
      return undefined;
    
    if (!this.queryResults.queryInfo.isOrderedBySingleField)
      return undefined;
    
    var asc: boolean = this.queryResults.queryInfo.orderedAsc;
    var orderedByColName: string = this.queryResults.queryInfo.orderedByColumnName;
    var colIdx: number = this.queryResults.columnNames.indexOf(orderedByColName);
    
    if (colIdx === -1)
      return undefined;
    
    var res: DiqubeTableOrderedCol = {
      colIdx: colIdx,
      asc: asc
    };
    
    if (!DiqubeUtil.equals(res, this.orderedColCache))
      this.orderedColCache = res;
    
    return this.orderedColCache;
  }
  
  public orderedColChangeRequest(requested: DiqubeTableOrderedCol): void {
    console.log("Requested to order by", requested);
  }
}