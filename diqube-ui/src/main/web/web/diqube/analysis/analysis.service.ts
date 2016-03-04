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
import * as remoteData from "../remote/remote";
import {RemoteService, RemoteServiceExecutionCallback} from "../remote/remote.service";
import * as analysisData from "./analysis";
import {DiqubeUtil} from "../util/diqube.util";
import {AnalysisExecutionService} from "./execution/analysis.execution.service";
import {AnalysisStateService} from "./state/analysis.state.service";

/**
 * Interface that is responsible when the analysis service would like to re-navigate because the currently loaded analysis was changed.
 */
export interface AnalysisServiceRenavigator {
  /**
   * Re-navigate to the given analysis. Return true if other renavigators should be called (= if navigation was not successful). 
   */
  analysisServiceReloadNeeded(analysisId: string, analysisVersion: number): boolean;
}

/**
 * Service managing loading a specific analysis (version) and changing it.
 * 
 * This service needs at least one AnalysisServiceReloadNeededListener registered, as it might want to re-navigate when
 * changing the analysis.
 */
@Injectable()
export class AnalysisService {
  public loadedAnalysis: remoteData.UiAnalysis = undefined;
  public newestVersionOfAnalysis: number = undefined;
  
  private renavigators: Array<AnalysisServiceRenavigator> = [];
  
  constructor(private remoteService: RemoteService, private analysisExecutionService: AnalysisExecutionService,
              private analysisStateService: AnalysisStateService) {}
  
  /**
   * Register a AnalysisServiceRenavigator that can potentially navigate.
   */
  public registerRenvaigator(listener: AnalysisServiceRenavigator): void {
    this.renavigators.push(listener);
  }
  
  /**
   * Unregister a AnalysisServiceRenavigator.
   */
  public unregisterRenavigator(listener: AnalysisServiceRenavigator): void {
    for (var idx in this.renavigators) {
      if (this.renavigators[idx] === listener) {
        this.renavigators.splice(idx, 1);
        return;
      }
    }
  }
  
  private fireRenavigation(analysisId: string, analysisVersion: number): void {
    if (!this.renavigators.length)
      console.warn("Requested to change URL, but no listeners are registered! Target:", analysisId, analysisVersion);    
      
    for (var idx in this.renavigators) {
      if (!this.renavigators[idx].analysisServiceReloadNeeded(analysisId, analysisVersion))
        return;
    }
    
    console.warn("None of the listeners was able to re-navigate to", analysisId, analysisVersion);
  }
  
  /**
   * Sets an already available analysis as the loaded one.
   */
  public setLoadedAnalysis(analysis: remoteData.UiAnalysis): void {
    this.loadedAnalysis = analysis;
    
    // clean analysis to make sure that we do not access undefined somewhere.
    if (!this.loadedAnalysis.qubes)
      this.loadedAnalysis.qubes = [];
    for (var idx in this.loadedAnalysis.qubes)
      this.initializeReceivedQube(this.loadedAnalysis.qubes[idx]);
    
    if (!this.loadedAnalysis.slices)
      this.loadedAnalysis.slices = [];
    for (var idx in this.loadedAnalysis.slices)
      this.initializeReceivedSlice(this.loadedAnalysis.slices[idx]);
    
    // ensure the correct version in the analysis object and in the URL.
    this.setCurrentAnalysisVersion(analysis.version);
  }
  
  /**
   * Freshly loads an analysis with a specified ID from the server. If  "version" is not given, the newest version will 
   * be loaded.
   * 
   * Will not load anything if the currently loadedAnalysis is the requested one.
   * 
   * Will try to preserve any query results if the newly loaded analysis is simply a different version than the
   * previously loaded analysis.
   */
  public loadAnalysis(id: string, version?: number): Promise<remoteData.UiAnalysis> {
    var me: AnalysisService = this;
    return new Promise((resolveFinal: (a: remoteData.UiAnalysis)=>void, rejectFinal: (reason: string)=>void) => {
      if (me.loadedAnalysis && me.loadedAnalysis.id === id && version !== undefined && me.loadedAnalysis.version === version) {
        // request to load the same analysis that is loaded already.
        resolveFinal(me.loadedAnalysis);
        return;
      }
      
      var previousAnalysisWithSameId: remoteData.UiAnalysis = undefined;
      if (me.loadedAnalysis && me.loadedAnalysis.id === id)
        previousAnalysisWithSameId = me.loadedAnalysis;
      
      me.loadedAnalysis = undefined;
      me.newestVersionOfAnalysis = undefined;
  
      var newestVersionPromise: Promise<void> = new Promise((resolveNewestVersion: (a: void)=>void, rejectNewestVersion: (reason: string)=>void) => {
        if (version !== undefined) {
          // we do NOT load the newest version, but a specific one. Therefore: Check what the newest version is
          // to make it available correctly in me.newestVersionOfAnalysis.
          var data: remoteData.NewestAnalysisVersionJsonCommand = {
            analysisId: id
          };
          me.remoteService.execute(remoteData.NewestAnalysisVersionJsonCommandConstants.NAME, data, {
            data: (dataType: string, data: any) => {
              if (dataType === remoteData.AnalysisVersionJsonResultConstants.TYPE) {
                var v: remoteData.AnalysisVersionJsonResult = <remoteData.AnalysisVersionJsonResult>data;
                me.newestVersionOfAnalysis = v.analysisVersion;
              }
              return false;
            },
            exception: (msg: string) => {
              rejectNewestVersion("Error while loading analysis " + id + ": " + msg);
            },
            done: () => {
              resolveNewestVersion(null);
            }
          });
        } else
          // we load the newest version anyway, will update newestVersion automatically below.
          resolveNewestVersion(null);
      });
      
      newestVersionPromise.then((a: void) => {
        var data: remoteData.AnalysisJsonCommand = {
          analysisId: id,
          analysisVersion: version // may be undefined
        };
        me.remoteService.execute(remoteData.AnalysisJsonCommandConstants.NAME, data, {
          data: (dataType: string, data: any) => {
            if (dataType === remoteData.AnalysisJsonResultConstants.TYPE) {
              var d: remoteData.AnalysisJsonResult = <remoteData.AnalysisJsonResult>data;
              if (!me.newestVersionOfAnalysis)
                // in case we loaded the newest version or version loading had an exception.
                me.newestVersionOfAnalysis = d.analysis.version;
              me.setLoadedAnalysis(d.analysis);
              
              if (previousAnalysisWithSameId)
                this.preserveResultsOfPreviousAnalysis(previousAnalysisWithSameId, me.loadedAnalysis);
              
              resolveFinal(me.loadedAnalysis);
            }
            return false;
          },
          exception: (msg: string) => {
            rejectFinal(msg);
          },
          done: () => {}
        });
      }).catch((msg: string) => {
        rejectFinal(msg);
      });
    });
  }
  
  /**
   * Unloads the currently loaded analysis
   */
  public unloadAnalysis(): void {
    this.loadedAnalysis = undefined;
    this.newestVersionOfAnalysis = undefined;
  }
  
  /**
   * Execute a specific query of the currently loaded analysis, see AnalysisExecutionService.
   */
  public provideQueryResults(qube: remoteData.UiQube, query: remoteData.UiQuery, 
                             intermediateResultsFn: (currentResults: analysisData.EnhancedTableJsonResult) => void): Promise<analysisData.EnhancedTableJsonResult> {
    if (!this.loadedAnalysis)
      return Promise.reject<analysisData.EnhancedTableJsonResult>("No analysis loaded");
    
    return this.analysisExecutionService.provideQueryResults(this.loadedAnalysis.id, this.loadedAnalysis.version, qube, 
      query, intermediateResultsFn);
  }
  
  /**
   * Clones the currently loaded analysis version into an analysis that is owned by the currently logged in user.
   * Will load the new analysis after it's created.
   */
  public cloneAndLoadCurrentAnalysis(): Promise<remoteData.UiAnalysis> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiAnalysis>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiAnalysis)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.CloneAnalysisJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version 
      };
      
      me.remoteService.execute(remoteData.CloneAnalysisJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.AnalysisJsonResultConstants.TYPE) {
            var a: remoteData.AnalysisJsonResult = <remoteData.AnalysisJsonResult>data;
            
            resolve(a.analysis);
            
            // redirect to new analysis.
            this.fireRenavigation(a.analysis.id, a.analysis.version);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }
  
  public addQube(name: string, sliceId: string): Promise<remoteData.UiQube> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiQube>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiQube)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.CreateQubeJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        name: name,
        sliceId: sliceId
      };
      
      me.remoteService.execute(remoteData.CreateQubeJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.QubeJsonResultConstants.TYPE) {
            var res: remoteData.QubeJsonResult = <remoteData.QubeJsonResult>data;

            me.initializeReceivedQube(res.qube);
            me.loadedAnalysis.qubes.push(res.qube);
            me.setCurrentAnalysisVersion(res.analysisVersion);
            
            resolve(res.qube);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }
  public addQuery(name: string, diql: string, qubeId: string): Promise<remoteData.UiQuery> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiQuery>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiQuery)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.CreateQueryJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        name: name,
        qubeId: qubeId,
        diql: diql
      };
      
      me.remoteService.execute(remoteData.CreateQueryJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.QueryJsonResultConstants.TYPE) {
            var res: remoteData.QueryJsonResult = <remoteData.QueryJsonResult>data;

            var qube: remoteData.UiQube = me.loadedAnalysis.qubes.filter((q) => { return q.id === qubeId })[0];
            qube.queries.push(res.query);
            
            this.analysisStateService.markToOpenQueryInEditModeNextTime(res.query.id);

            me.setCurrentAnalysisVersion(res.analysisVersion);
            resolve(res.query);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }
  public addSlice(name: string, manualConjunction: string, sliceDisjunctions: Array<remoteData.UiSliceDisjunction>): Promise<remoteData.UiSlice> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiSlice>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiSlice)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.CreateSliceJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        name: name,
        manualConjunction: manualConjunction,
        sliceDisjunctions: sliceDisjunctions
      };
      
      me.remoteService.execute(remoteData.CreateSliceJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.SliceJsonResultConstants.TYPE) {
            var res: remoteData.SliceJsonResult = <remoteData.SliceJsonResult>data;

            me.loadedAnalysis.slices.push(res.slice);
            
            this.analysisStateService.markToOpenSliceInEditModeNextTime(res.slice.id);

            me.setCurrentAnalysisVersion(res.analysisVersion);
            resolve(res.slice);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }

  /**
   * Helper method that provides a RemoteServiceExecutionCallback that can handle a QueryJsonResultConstants.TYPE.
   * 
   * The returned callback will try to preserve any $results of the query, if meaningful.
   * 
   * @param qubeId: The ID of the qube that the received query belongs to - that qube will be searched for incorporating the query!
   * @param resolve: The resolve method of e.g. a Promise that will be called as soon as the data has been received.
   * @param reject: The reject method of e.g. a Promise that will be called if anything went wrong.
   */
  private createQueryRemoteServiceExecutionCallback(qubeId: string, resolve: (a: remoteData.UiQuery)=>void, 
                                                    reject: (reason: string)=>void): RemoteServiceExecutionCallback {
    var me = this;
    return {
      data: (dataType: string, data: any) => {
        if (dataType === remoteData.QueryJsonResultConstants.TYPE) {
          var res: remoteData.QueryJsonResult = <remoteData.QueryJsonResult>data;

          var oldQuery: remoteData.UiQuery = undefined;
          
          for (var qubeIdx in me.loadedAnalysis.qubes) {
            if (me.loadedAnalysis.qubes[qubeIdx].id === qubeId) {
              var qube: remoteData.UiQube = me.loadedAnalysis.qubes[qubeIdx];
              for (var queryIdx in qube.queries) {
                if (qube.queries[queryIdx].id === res.query.id) {
                  oldQuery = qube.queries[queryIdx];
                  break; 
                }
              }
            }
            if (oldQuery)
              break;
          }
          
          if (!oldQuery) {
            console.warn("Could not find the query that should be replaced by the updated query. " + 
                         "Did the server change the query ID?");
            reject("Internal error. Please refresh the page.");
            return;
          }
                  
          if (oldQuery.diql !== res.query.diql) {
            // be sure to cancel execution if the query executes based on old properties
            this.analysisExecutionService.cancelQueryIfRunning(oldQuery);
            // remove any results we might have already
            if (analysisData.isUiQueryWithResults(oldQuery))
              analysisData.removeResultsFromUiQueryWithResults((<analysisData.UiQueryWithResults>oldQuery));
          }
          
          // overwrite everything of oldQuery with value of the received query. Do not replace object, since then
          // angular would replace the UI elements with new ones and we would e.g. lose edit-mode state
          DiqubeUtil.copyTo(res.query, oldQuery);

          me.setCurrentAnalysisVersion(res.analysisVersion);
          resolve(oldQuery);
        }
        return false;
      },
      exception: (msg: string) => {
        reject(msg);
      },
      done: () => {}
    };
  }
  
  /**
   * Sends an updated version of a query to the server. Note that the passed query object should not yet be the 
   * one that is reachable from me.loadedAnalysis, as the changes will be incorporated into that object when the
   * resulting query is received from the server after updating.
   * 
   * If possible, the query.$results will be preserved in the new query object, but it could be that they are
   * removed and need to be re-queried using #provideQueryResults.
   */
  public updateQuery(qubeId: string, query: remoteData.UiQuery): Promise<remoteData.UiQuery> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiQuery>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiQuery)=>void, reject: (reason: string)=>void) => {
      var cleanDiql: string = query.diql;
      if (cleanDiql) {
        // Chrome seems to sometimes send bytes "c2 a0" (which is encoded unicode &nbsp;, in unicode \u00a0). 
        // Our parser does not like this, so replace it with a regular space.
        cleanDiql = cleanDiql.replace(/\xc2\xa0/g, " ");
        cleanDiql = cleanDiql.replace(/\u00a0/g, " ");
      }
      
      var queryToSend: remoteData.UiQuery = DiqubeUtil.copy(query);
      queryToSend.diql = cleanDiql;
      
      var data: remoteData.UpdateQueryJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        qubeId: qubeId,
        newQuery: queryToSend
      };
      
      me.remoteService.execute(remoteData.UpdateQueryJsonCommandConstants.NAME, data, 
                               me.createQueryRemoteServiceExecutionCallback(qubeId, resolve, reject));
    });
  }
  
  /**
   * Requests the server to change the "ORDER BY" clause of a query.
   * 
   * If possible, the query.$results will be preserved in the new query object, but it could be that they are
   * removed and need to be re-queried using #provideQueryResults.
   * 
   * @param orderByRequest: the "request string" that can be used in the ORDER BY directly (not a column name, but the 
   *                        request used to request a column/projected column/aggregated column etc).
   */
  public adjustQueryOrdering(qubeId: string, queryId: string, orderByRequest: string, orderAsc: boolean): Promise<remoteData.UiQuery> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiQuery>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiQuery)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.AdjustQueryOrderingJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        qubeId: qubeId,
        queryId: queryId,
        orderByRequest: orderByRequest,
        orderAsc: orderAsc
      };
      
      me.remoteService.execute(remoteData.AdjustQueryOrderingJsonCommandConstants.NAME, data, 
                               me.createQueryRemoteServiceExecutionCallback(qubeId, resolve, reject));
    });
  }
  
  /**
   * Stores an updated qube on the server. Note that the new qube should not be reachable from loadedAnalysis, but
   * this method will do integrate the new qube after the server responded with success.
   * 
   * After the command completed, the queries of the qube might need to be re-executed (if the slice of the qube
   * changed). This method will automatically remove the $results objects in the queries in that case.
   * 
   * Note that this method will update only a few properties of a qube on the server - changes on queries are
   * handled by separate methods!
   */
  public updateQube(newQube: remoteData.UiQube): Promise<remoteData.UiQube> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiQube>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiQube)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.UpdateQubeJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        qubeId: newQube.id,
        qubeName: newQube.name,
        sliceId: newQube.sliceId
      };
      
      me.remoteService.execute(remoteData.UpdateQubeJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.QubeJsonResultConstants.TYPE) {
            var res: remoteData.QubeJsonResult = <remoteData.QubeJsonResult>data;

            var foundQubeIdx: number = undefined;
            for (var idx in me.loadedAnalysis.qubes) {
              if (me.loadedAnalysis.qubes[idx].id === res.qube.id) {
                foundQubeIdx = idx;
                break;
              }
            }
            
            if (foundQubeIdx === undefined) {
              console.warn("Could not find the qube that should be replaced by the updated qube. Did the server " +
                           "change the query ID?");
              reject("Internal error. Please refresh the page.");
              return;
            }
            
            var oldQube: remoteData.UiQube = me.loadedAnalysis.qubes[foundQubeIdx];
            
            if (oldQube.sliceId !== res.qube.sliceId) {
              // slice was changed, so remove all results of all queries and cancel their execution.
              for (var queryIdx in oldQube.queries) {
                var query = oldQube.queries[queryIdx];
                if (analysisData.isUiQueryWithResults(query)) {
                  analysisData.removeResultsFromUiQueryWithResults(<analysisData.UiQueryWithResults>query);
                }
                // cancel if something is running
                this.analysisExecutionService.cancelQueryIfRunning(query);
              }
            }
            
            // overwrite everything of oldQuebe with value of the received qube. Do not replace object, since then
            // angular would replace the UI elements with new ones and we would e.g. lose edit-mode state
            DiqubeUtil.copyTo(res.qube, oldQube);

            me.setCurrentAnalysisVersion(res.analysisVersion);
            resolve(oldQube);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }
  
  /**
   * Sends an updated version of a slice to the server. Note that the passed slice object should not yet be the 
   * one that is reachable from me.loadedAnalysis, as the changes will be incorporated into that object when the
   * resulting slice is received from the server after updating.
   * 
   * Note that this method will remove the query.$results objects of all queries that are connected to the slice
   * by their qube (only if slices selection properties changed). The results might therefore need to be
   * re-calculated!
   */
  public updateSlice(newSlice: remoteData.UiSlice): Promise<remoteData.UiSlice> {
    if (!this.loadedAnalysis)
      return Promise.reject<remoteData.UiSlice>("No analysis loaded");
    
    var me: AnalysisService = this;
    return new Promise((resolve: (a: remoteData.UiSlice)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.UpdateSliceJsonCommand = {
        analysisId : me.loadedAnalysis.id, 
        analysisVersion: me.loadedAnalysis.version,
        slice: newSlice
      };
      
      me.remoteService.execute(remoteData.UpdateSliceJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) => {
          if (dataType === remoteData.SliceJsonResultConstants.TYPE) {
            var res: remoteData.SliceJsonResult = <remoteData.SliceJsonResult>data;

            var origSlice: remoteData.UiSlice = undefined;
            for (var sliceIdx in me.loadedAnalysis.slices) {
              if (me.loadedAnalysis.slices[sliceIdx].id === res.slice.id) {
                origSlice = me.loadedAnalysis.slices[sliceIdx];
                break;
              }
            }
            
            if (!origSlice) {
              console.warn("Could not find the slice that should be replaced by the updated slice. " + 
                  "Did the server change the slice ID?");
              reject("Internal error. Please refresh the page.");
              return;
            }
            
            if (!DiqubeUtil.equals(origSlice.manualConjunction, res.slice.manualConjunction) || 
                !DiqubeUtil.equals(origSlice.sliceDisjunctions, res.slice.sliceDisjunctions)) {
              for (var qubeIdx in me.loadedAnalysis.qubes) {
                var qube = me.loadedAnalysis.qubes[qubeIdx];
                if (qube.sliceId === res.slice.id) {
                  // clean $results
                  for (var queryIdx in qube.queries) {
                    if (analysisData.isUiQueryWithResults(qube.queries[queryIdx]))
                      analysisData.removeResultsFromUiQueryWithResults(<analysisData.UiQueryWithResults>qube.queries[queryIdx]);
                    this.analysisExecutionService.cancelQueryIfRunning(qube.queries[queryIdx]);
                  }
                }
              }
            }

            // overwrite everything of origSlice with value of the received slice. Do not replace object, since then
            // angular would replace the UI elements with new ones and we would e.g. lose edit-mode state
            DiqubeUtil.copyTo(res.slice, origSlice);

            me.setCurrentAnalysisVersion(res.analysisVersion);
            resolve(origSlice);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {}
      });
    });
  }
  
  /**
   * Deletes a query
   */
  public removeQuery(qubeId: string, queryId: string): Promise<void> {
    if (!this.loadedAnalysis)
      return Promise.reject<void>("No analysis loaded");

    var me: AnalysisService = this;
    
    return new Promise((resolve: (a: void)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.RemoveQueryJsonCommand = {
        analysisId: me.loadedAnalysis.id,
        analysisVersion: me.loadedAnalysis.version,
        qubeId: qubeId,
        queryId: queryId
      };
      
      me.remoteService.execute(remoteData.RemoveQueryJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) =>  {
          if (dataType === remoteData.AnalysisVersionJsonResultConstants.TYPE) {
            var res: remoteData.AnalysisVersionJsonResult = <remoteData.AnalysisVersionJsonResult>data;
            me.setCurrentAnalysisVersion(res.analysisVersion);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {
          var qube: remoteData.UiQube = me.loadedAnalysis.qubes.filter((q) => { return q.id === qubeId; })[0];
          
          var removedQuery: boolean = false;
          for (var queryIdx in qube.queries) {
            var query: remoteData.UiQuery = qube.queries[queryIdx];
            
            if (query.id === queryId) {
              qube.queries.splice(queryIdx, 1);
              removedQuery = true;
              break;
            }
          }
          
          if (!removedQuery) { 
            console.warn("Could not find the query that should have been removed.");
            reject("Internal error. Please refresh the page.");
            return;
          }
          
          resolve(null);
        }
      });
    });
  }
  
  /**
   * Deletes a qube
   */
  public removeQube(qubeId: string): Promise<void> {
    if (!this.loadedAnalysis)
      return Promise.reject<void>("No analysis loaded");

    var me: AnalysisService = this;
    
    return new Promise((resolve: (a: void)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.RemoveQubeJsonCommand = {
        analysisId: me.loadedAnalysis.id,
        analysisVersion: me.loadedAnalysis.version,
        qubeId: qubeId
      };
      
      me.remoteService.execute(remoteData.RemoveQubeJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) =>  {
          if (dataType === remoteData.AnalysisVersionJsonResultConstants.TYPE) {
            var res: remoteData.AnalysisVersionJsonResult = <remoteData.AnalysisVersionJsonResult>data;
            me.setCurrentAnalysisVersion(res.analysisVersion);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {
          var removedQube: boolean = false;
          for (var qubeIdx in me.loadedAnalysis.qubes) {
            var qube: remoteData.UiQube = me.loadedAnalysis.qubes[qubeIdx];
            
            if (qube.id === qubeId) {
              me.loadedAnalysis.qubes.splice(qubeIdx, 1);
              removedQube = true;
              break;
            }
          }
          
          if (!removedQube) { 
            console.warn("Could not find the qube that should have been removed.");
            reject("Internal error. Please refresh the page.");
            return;
          }
          
          resolve(null);
        }
      });
    });
  }
  
  /**
   * Deletes a slice.
   */
  public removeSlice(sliceId: string): Promise<void> {
    if (!this.loadedAnalysis)
      return Promise.reject<void>("No analysis loaded");

    var me: AnalysisService = this;
    
    return new Promise((resolve: (a: void)=>void, reject: (reason: string)=>void) => {
      var data: remoteData.RemoveSliceJsonCommand = {
        analysisId: me.loadedAnalysis.id,
        analysisVersion: me.loadedAnalysis.version,
        sliceId: sliceId
      };
      
      me.remoteService.execute(remoteData.RemoveSliceJsonCommandConstants.NAME, data, {
        data: (dataType: string, data: any) =>  {
          if (dataType === remoteData.AnalysisVersionJsonResultConstants.TYPE) {
            var res: remoteData.AnalysisVersionJsonResult = <remoteData.AnalysisVersionJsonResult>data;
            me.setCurrentAnalysisVersion(res.analysisVersion);
          }
          return false;
        },
        exception: (msg: string) => {
          reject(msg);
        },
        done: () => {
          var removedSlice: boolean = false;
          for (var sliceIdx in me.loadedAnalysis.slices) {
            var slice: remoteData.UiSlice = me.loadedAnalysis.slices[sliceIdx];
            
            if (slice.id === sliceId) {
              me.loadedAnalysis.slices.splice(sliceIdx, 1);
              removedSlice = true;
              break;
            }
          }
          
          if (!removedSlice) { 
            console.warn("Could not find the slice that should have been removed.");
            reject("Internal error. Please refresh the page.");
            return;
          }
          
          resolve(null);
        }
      });
    });
  }
  
  private initializeReceivedQube(qube: remoteData.UiQube): void {
    if (!qube.queries)
      qube.queries = [];
  }
  
  private initializeReceivedSlice(slice: remoteData.UiSlice): void {
    if (!slice.sliceDisjunctions)
      slice.sliceDisjunctions = [];
  }
  
  private setCurrentAnalysisVersion(newVersion: number) {
    this.loadedAnalysis.version = newVersion;
    if (!this.newestVersionOfAnalysis || newVersion > this.newestVersionOfAnalysis)
      this.newestVersionOfAnalysis = newVersion;

    // push new URL
    this.fireRenavigation(this.loadedAnalysis.id, this.loadedAnalysis.version);
  }
  
  
  /**
   * Tries to preserve some query results. This is meaningful if the previousAnalysis is actually just a
   * different version of the same analysis, which means that a lot of queries might actually be equal and we do
   * not need to calculate them anymore. This method identifies the queries that are the same between two analysis
   * and copies potentially available results to the new one.
   */
  private preserveResultsOfPreviousAnalysis(previousAnalysis: remoteData.UiAnalysis, newAnalysis: remoteData.UiAnalysis): void {
    if (!previousAnalysis || !newAnalysis)
      return;
    if (previousAnalysis.id !== newAnalysis.id)
      // does make sense to only compare two versions of the same analysis.
      return;
    
    var equalSliceIds: Array<string> = []; // "equal" means: same restrictions. If the name is different, that does not matter.
    for (var prevSliceIdx in previousAnalysis.slices) {
      var prevSlice: remoteData.UiSlice = previousAnalysis.slices[prevSliceIdx];
      var newSlices: Array<remoteData.UiSlice> = newAnalysis.slices.filter((s: remoteData.UiSlice) => { return s.id === prevSlice.id; });
      if (newSlices && newSlices.length) {
        var newSlice: remoteData.UiSlice = newSlices[0];
        
        // ok, we have a slice with the same ID in prev and new.
        if (DiqubeUtil.equals(prevSlice.sliceDisjunctions, newSlice.sliceDisjunctions) && 
            DiqubeUtil.equals(prevSlice.manualConjunction, newSlice.manualConjunction))
          equalSliceIds.push(prevSlice.id);
      }
    }
    
    var preservedQueryResults: number = 0;
    
    for (var prevQubeIdx in previousAnalysis.qubes) {
      var prevQube: remoteData.UiQube = previousAnalysis.qubes[prevQubeIdx];
      if (equalSliceIds.indexOf(prevQube.sliceId) > -1) {
        var newQubes: Array<remoteData.UiQube> = newAnalysis.qubes.filter((q: remoteData.UiQube) => { return q.id === prevQube.id; });
        if (newQubes && newQubes.length) {
          // Ok, we found a qube that (1) has a slice that is equal in prev and new and (2) that is available in prev and new.
          var newQube: remoteData.UiQube = newQubes[0];
          
          for (var prevQueryIdx in prevQube.queries) {
            var prevQuerySimple: remoteData.UiQuery = prevQube.queries[prevQueryIdx];
            
            if (!analysisData.isUiQueryWithResults(prevQuerySimple))
              // if we do not have any results, we will not be able to copy anything, therefore skip right away.
              continue;
            
            var prevQuery: analysisData.UiQueryWithResults = <analysisData.UiQueryWithResults>prevQuerySimple;
            
            var newQueries = newQube.queries.filter((q: remoteData.UiQuery) => { return q.id === prevQuery.id; });
            if (newQueries && newQueries.length) {
              var newQuery: remoteData.UiQuery = newQueries[0];
              // Ok, we found that both prev and new contain a query with the same Id.
              
              if (DiqubeUtil.equals(newQuery.diql, prevQuery.diql)) {
                // wohoo, we found equal queries in prev and new!
                analysisData.enhanceUiQueryWithResults(newQuery, prevQuery.$results);
                preservedQueryResults = preservedQueryResults + 1;
              }
            }
          }
        }
      }
    }
  }
}