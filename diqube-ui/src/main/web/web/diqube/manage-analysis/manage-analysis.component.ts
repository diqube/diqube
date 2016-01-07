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
import {AnalysisRefJsonResult, AnalysisRefJsonResultConstants, ListAllAnalysisJsonCommandConstants} from "../remote/remote";
import {RemoteService} from "../remote/remote.service";
import {LoginStateService} from "../login-state/login-state.service";

@Component({
    selector: "diqube-manage-analysis",
    templateUrl: "diqube/manage-analysis/manage-analysis.html"
})
export class ManageAnalysisComponent implements OnInit {
  public allAnalysis: AnalysisRefJsonResult[] = [];
  public reloading: boolean = false;
  
  constructor(private remoteService: RemoteService, private router: Router, private loginStateService: LoginStateService) { }
  
  public ngOnInit(): any {
    if (!this.loginStateService.isTicketAvailable())
      this.loginStateService.loginAndReturnHere();
    
    this.reloadAnalysis();
  }
  
  public reloadAnalysis(): void {
    if (this.reloading)
      return;
    
    this.allAnalysis = [];
    this.reloading = true;
    var me: ManageAnalysisComponent = this;
    this.remoteService.execute(ListAllAnalysisJsonCommandConstants.NAME, null, {
      data: (dataType: string, data: any) => {
        if (dataType === AnalysisRefJsonResultConstants.TYPE) {
          this.allAnalysis.push(<AnalysisRefJsonResult>data);
        }
        return false;
      },
      exception: (msg: string) => {
        me.reloading = false;
      },
      done: () => {
        me.reloading = false;
      } 
    });
  }

  public openAnalysis(analysis: AnalysisRefJsonResult): void {
    // TODO
  }
  
  public createAnalysis(): void {
    this.router.navigate([ "CreateAnalysis" ]);
  }
  
}
