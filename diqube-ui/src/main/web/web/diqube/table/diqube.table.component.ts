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

import {Component, Input, OnInit} from "angular2/core";
import {DiqubeUtil} from "../util/diqube.util";

@Component({
  selector: "diqube-table",
  templateUrl: "diqube/table/table.html"
})
export class DiqubeTableComponent implements OnInit {
  /** Names of the columns to be displayed */
  @Input("columns") public columns: Array<string>;
  
  /**
   * For each row there is an inner array containing the data of each column.
   */
  @Input("rows") public rows: Array<Array<any>>;
  
  /**
   * Those indices in #columns of the columns that have "string" values.
   */
  @Input("stringColumnIndices") public stringColumnIndices: Array<number>;
  
  /**
   * "true" if empty columns of the same row should be colspaned together.
   */
  @Input("colspanEmptyCols") public colspanEmptyCols: string;
  
  /**
   * Indices of those columns where equal values should be rowspanned.
   */
  @Input("rowspanCols") public rowspanCols: Array<string>;
  
  public elementId: string;
  private cachedFinalRes: Array<Array<{rowspan: number, colspan: number, value: any}>> = undefined;
  
  constructor() {
    this.elementId = DiqubeUtil.newUuid();
  }
  
  public ngOnInit(): any {
    var tableElement = document.getElementById(this.elementId);
    if (!tableElement) {
      // if executed before the ID is inside the html element, try again in next tick.
      setTimeout(() => {
        this.ngOnInit();
      }, 0);
      return;
    }
    
    DiqubeUtil.mdlComponentHandler().upgradeElement(tableElement);
  }
  
  public columnIndexIsStringColumn(idx: number): boolean {
    return !this.stringColumnIndices || this.stringColumnIndices.indexOf(idx) > -1;
  }
  
  public finalRows(): Array<Array<{rowspan: number, colspan: number, value: any}>> {
    var res: Array<Array<{rowspan: number, colspan: number, value: any}>> = [];
    
    for (var rowIdx in this.rows) {
      var row = this.rows[rowIdx];
      var curBaseIdx = 0;
      var curColSpanLen = 1;
      var resRow : Array<{rowspan: number, colspan: number, value: any}> = [];
      for (var colIdx = 1; colIdx < row.length; colIdx++) {
        var cell = row[colIdx];
        if (this.colspanEmptyCols === "true" && (cell === undefined || cell === null || cell === "")) {
          curColSpanLen++;
        } else {
          resRow.push({rowspan: 1, colspan: curColSpanLen, value: row[curBaseIdx]});
          curColSpanLen = 1;
          curBaseIdx = colIdx;
        }
      }
      resRow.push({rowspan: 1, colspan: curColSpanLen, value: row[curBaseIdx]});
      res.push(resRow);
    }
    
    if (this.rowspanCols) {
      for (var i in this.rowspanCols) {
        var rowspanColIdx: number = parseInt(this.rowspanCols[i]);
        var previousValue: {rowspan: number, colspan: number, value: any} = undefined;
        var startIdx: number = -1;
        for (var rowIdx in res) {
          rowIdx = parseInt(rowIdx);
          if (!DiqubeUtil.equals(res[rowIdx][rowspanColIdx], previousValue)) {
            if (startIdx >= 0) {
              res[startIdx][rowspanColIdx].rowspan = rowIdx - startIdx;
              for (var removeIdx = startIdx + 1; removeIdx < rowIdx; removeIdx++) {
                // remove the rowspanned elements.
                res[removeIdx].splice(rowspanColIdx, 1);
              }
            }
            
            startIdx = rowIdx;
            previousValue = res[rowIdx][rowspanColIdx]; 
          }
        }
        // the one after the last is different from the last.
        rowIdx++;
        res[startIdx][rowspanColIdx].rowspan = rowIdx - startIdx;
        for (var removeIdx = startIdx + 1; removeIdx < rowIdx; removeIdx++) {
          // remove the rowspanned elements.
          res[removeIdx].splice(rowspanColIdx, 1);
        }
      }
    }
    
    if (!DiqubeUtil.equals(this.cachedFinalRes, res))
      this.cachedFinalRes = res;
    
    return this.cachedFinalRes;
  }
}