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

import {Component, OnInit} from "angular2/core";
import {Router} from "angular2/router";
import {StatsJsonResult, StatsJsonResultConstants, TableJsonResult, TableJsonResultConstants, PlainQueryJsonCommand, 
        PlainQueryJsonCommandConstants} from "../remote/remote";
import {RemoteService} from "../remote/remote.service";
import {LoginStateService} from "../login-state/login-state.service";
import {IterateMapSortedPipe, LimitToPipe} from "../util/diqube.pipes";

@Component({
  selector: "diqube-single-query",
  templateUrl: "diqube/query/query.html",
  pipes: [ IterateMapSortedPipe, LimitToPipe ]
})
export class QueryComponent implements OnInit {
  private static DISPLAY_RESULTS = "results";
  private static DISPLAY_STATS = "stats";  
  
  public diql: string = "";
  public result: TableJsonResult = undefined;
  public stats: StatsJsonResult = undefined;
  public exception: string = undefined;
  public isExecuting: boolean = false;

  private displayResultsOrStats: string = undefined;
  private lastPercentComplete: number = -1;
  private lastRequestId: string = undefined;
  
  constructor(private loginStateService: LoginStateService, private remoteService: RemoteService) {}
  
  public static navigate(router: Router) {
    router.navigate([ "/Query" ]);
  }
  
  public ngOnInit(): any {
    if (!this.loginStateService.isTicketAvailable())
      this.loginStateService.loginAndReturnHere();
  }
  
  public execute(): void {
    if (this.isExecuting)
      return;
    
    this.result = undefined;
    this.stats = undefined;
    this.exception = undefined;
    this.displayResultsOrStats = undefined;
    
    this.lastPercentComplete = -1;
    this.isExecuting = true;
    var data: PlainQueryJsonCommand = {
      diql: this.diql
    };
    var me: QueryComponent = this;
    this.lastRequestId = this.remoteService.execute(PlainQueryJsonCommandConstants.NAME, data, {
      data: (dataType: string, data: any) => {
        if (dataType === TableJsonResultConstants.TYPE) {
          var tab: TableJsonResult = <TableJsonResult>data;
          if (tab.percentComplete > me.lastPercentComplete) {
            me.result = tab;
            me.displayResultsOrStats = QueryComponent.DISPLAY_RESULTS;
            me.lastPercentComplete = tab.percentComplete;
          }
        } else if (dataType === StatsJsonResultConstants.TYPE) {
          var stats: StatsJsonResult = <StatsJsonResult>data;
          me.stats = stats;
        }  
        return false;
      },
      exception: (msg: string) => {
        me.isExecuting = false;
        me.exception = msg;
        me.result = undefined;
        me.stats = undefined;
      },
      done: () => {
        me.isExecuting = false;
      } 
    });
  }
  
  public cancel(): void {
    if (!this.isExecuting)
      return;
    this.remoteService.cancel(this.lastRequestId);
    this.lastRequestId = null;
    this.result = null;
    this.exception = null;
    this.stats = null;
    this.displayResultsOrStats = undefined;

    this.isExecuting = false;
  }
  
  public switchToDisplayResults(): void {
    this.displayResultsOrStats = QueryComponent.DISPLAY_RESULTS;
  }
  
  public switchToDisplayStats(): void {
    this.displayResultsOrStats = QueryComponent.DISPLAY_STATS;
  }
  
  public displayResults(): boolean {
    return this.displayResultsOrStats == QueryComponent.DISPLAY_RESULTS;
  }

  public displayStats(): boolean {
    return this.displayResultsOrStats == QueryComponent.DISPLAY_STATS;
  }
  
  public displayAnything(): boolean {
    return this.displayResultsOrStats !== undefined;
  }
}