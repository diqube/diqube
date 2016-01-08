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
import * as remoteData from "../../remote/remote";
import * as analysisData from "../analysis";
import {RemoteService} from "../../remote/remote.service";

@Injectable()
export class AnalysisExecutionService {
    
  constructor(private remoteService: RemoteService) {}
    
  private runningQueries: Array<{queryId: string, requestId: string}> = [];
  
  /**
   * Loads a field "$results" into the query object which is updated continuously until it contains the full
   * results of executing the query (field is defined in UiQueryWithResults). With each new intermediate update
   * available, the optional intermediateResultsFn will be called.
   * 
   * If there are results available already (query.$results !== undefined), the results will not be loaded again.
   * 
   * Note that the returned Promise will return one of those "result objects" even on a call to "reject"!
   * 
   * @param qube The qube of the query to execute
   * @param query The query to execute
   * @param intermediateResultsFn function(resultsObj): called when intermediate results are available. Can be undefined. This will only be called asynchronously.
   */
  public provideQueryResults(analysisId: string, analysisVersion: number, qube: remoteData.UiQube, inputQuery: remoteData.UiQuery, 
                             intermediateResultsFn: (currentResults: analysisData.EnhancedTableJsonResult) => void): Promise<analysisData.EnhancedTableJsonResult> {
    if (analysisData.isUiQueryWithResults(inputQuery))
      // there are results available already, maybe another instance of this method is currently executing it.
      return Promise.resolve((<analysisData.UiQueryWithResults>inputQuery).$results);
    
    // If someone just set query.$results to undefined and wants to re-execute, there might be another query 
    // running anyway. Cancel that one.
    this.cancelQueryIfRunning(inputQuery);

    var query: analysisData.UiQueryWithResults = analysisData.enhanceUiQueryWithResults(inputQuery, 
      { percentComplete: 0, 
        rows: undefined, 
        columnRequests: undefined,
        columnNames: undefined });
    
    if (intermediateResultsFn) {
      intermediateResultsFn(query.$results);              
    }
    
    var me: AnalysisExecutionService = this;
    return new Promise((resolve: (a: analysisData.EnhancedTableJsonResult)=>void, reject: (reason: analysisData.EnhancedTableJsonResult)=>void) => {
      var data: remoteData.AnalysisQueryJsonCommand = {
        analysisId: analysisId,
        analysisVersion: analysisVersion,
        qubeId: qube.id,
        queryId: query.id
      };
      
      var requestId: string = me.remoteService.execute(remoteData.AnalysisQueryJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.TableJsonResultConstants.TYPE && !query.$results.exception) {
            var table: remoteData.TableJsonResult = <remoteData.TableJsonResult>data;
            
            if (table.percentComplete >= query.$results.percentComplete) {
              query.$results.rows = table.rows;
              query.$results.columnNames = table.columnNames;
              query.$results.columnRequests = table.columnRequests;
              query.$results.percentComplete = table.percentComplete;
            }
            if (intermediateResultsFn)
              intermediateResultsFn(query.$results);
          }
          return false;
        },
        exception: (msg: string) => {
          this.popRunningQuery(query.id);
          
          query.$results.exception = msg;
          reject(query.$results);
        },
        done: () => {
          this.popRunningQuery(query.id);
          
          query.$results.percentComplete = 100;
          resolve(query.$results);
        }
      });
      
      this.runningQueries.push({
        queryId: query.id,
        requestId: requestId
      });
    });
  }
  
  /**
   * Cancel the execution of a UiQuery, if one is being executed currently.
   */
  public cancelQueryIfRunning(query: remoteData.UiQuery): void {
    var executionInfo: { queryId: string, requestId: string } = this.popRunningQuery(query.id);
    
    if (executionInfo === undefined)
      return;
    
    this.remoteService.cancel(executionInfo.requestId);
    
    if (analysisData.isUiQueryWithResults(query))
      (<analysisData.UiQueryWithResults>query).$results.exception = "Cancelled.";
  }
  
  /**
   * Finds and removes the information on the running query, if running.
   */
  private popRunningQuery(queryId: string): { queryId: string, requestId: string } {
    var foundIdx: number = undefined;
    for (var idx in this.runningQueries) {
      if (this.runningQueries[idx].queryId === queryId) {
        foundIdx = idx;
        break;
      }
    }
    
    if (foundIdx === undefined)
      return undefined;

    var res: { queryId: string, requestId: string } = this.runningQueries[foundIdx];
    this.runningQueries.splice(foundIdx, 1);

    return res;
  }
}