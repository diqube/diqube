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
import {StatsJsonResult} from "../remote/remote";
import {DiqubeTableComponent} from "../table/diqube.table.component";
import {DiqubeUtil} from "../util/diqube.util";

/**
 * Component capable of displaying StatsJsonResult.
 */
@Component({
  selector: "diqube-query-stats",
  templateUrl: "diqube/stats/stats.html",
  directives: [ DiqubeTableComponent ]
})
export class DiqubeStatsComponent {
    
  @Input("stats") public stats: StatsJsonResult = undefined;
  
  private statsRowsCache: Array<Array<any>> = undefined;
  private statsColsCache: Array<any> = undefined;
  
  public statsRows(): Array<Array<any>> {
    var res: Array<Array<any>> = [];
    
    res.push(this.statsRow("startedUntilDoneMs"));
    res.push(this.statsRow("numberOfThreads"));
    res.push(this.statsRow("numberOfTemporaryColumnShardsCreated"));
    res.push(this.statsRow("numberOfTemporaryColumnShardsFromCache"));
    res.push(this.statsRow("numberOfPagesInTable"));
    res.push(this.statsRow("numberOfTemporaryPages"));
    
    var complexValues = this.complexStatsRows("numberOfPageAccesses");
    for (let i in complexValues)
      res.push(complexValues[i]);
    
    complexValues = this.complexStatsRows("numberOfTemporaryPageAccesses");
    for (let i in complexValues)
      res.push(complexValues[i]);
    
    complexValues = this.complexStatsRows("numberOfTemporaryVersionsPerColName");
    for (let i in complexValues)
      res.push(complexValues[i]);

    complexValues = this.complexStatsRows("stepsActiveMs");
    for (let i in complexValues)
      res.push(complexValues[i]);

    if (!DiqubeUtil.equals(this.statsRowsCache, res))
      this.statsRowsCache = res;

    return this.statsRowsCache;
  }
  
  private statsRow(fieldName: string): Array<any> {
    var row: Array<any> = [];
    row.push(fieldName);
    row.push(undefined);
    for (var remoteIdx in this.stats.nodeNames) {
      row.push((<any>this.stats)[fieldName][remoteIdx]);
    }
    return row;
  }
  
  private complexStatsRows(mapName: string): Array<Array<any>> {
    var value = (<any>this.stats)[mapName];
    
    var keys: Array<any> = [];
    for (var key in value) {
      if (value.hasOwnProperty(key)) {
        keys.push(key);
      }
    }
    
    keys.sort();
   
    var res: Array<Array<any>> = [];
    
    for (var keyIdx in keys) {
      var row: Array<any> = [];
      row.push(mapName);
      var key = keys[keyIdx];
      row.push(this.limitLength(key, 100));
      for (var remoteIdx in this.stats.nodeNames)
        row.push((<any>this.stats)[mapName][key][remoteIdx]);
      res.push(row);
    }
    return res;
  }
  
  public statsCols(): Array<any> {
    var res: Array<any> = [ "", "" ];
    for (var i in this.stats.nodeNames)
      res.push(this.stats.nodeNames[i]);

    if (!DiqubeUtil.equals(res, this.statsColsCache))
      this.statsColsCache = res;
  
    return this.statsColsCache;
  }
  
  private limitLength(str: string, len: number): string {
    if (str.length > len)
      return str.substr(0, len);
    return str;
  }
 
}