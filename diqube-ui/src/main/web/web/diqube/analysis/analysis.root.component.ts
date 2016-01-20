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
import {RouteConfig, ROUTER_DIRECTIVES, RouteParams, Router} from "angular2/router";
import {AnalysisManageComponent} from "./manage/analysis.manage.component"
import {AnalysisCreateComponent} from "./create/analysis.create.component"
import {RemoteService} from "../remote/remote.service";
import * as remoteData from "../remote/remote";
import {AnalysisMainComponent} from "./analysis.main.component";
import {LoginStateService} from "../login-state/login-state.service";
import {DiqubeBaseNavigatableComponent} from "../diqube.base.component";

@Component({
  selector: "diqube-analysis-newest",
  template: "<div></div>",
})
export class AnalysisNewestComponent extends DiqubeBaseNavigatableComponent implements OnInit {
  public static ROUTE_PARAM_ANALYSIS_ID: string = "analysisId";  
  
  constructor(private routeParams: RouteParams, private remoteService: RemoteService, private router: Router, 
              loginStateService: LoginStateService) {
    super(true, undefined, loginStateService, undefined);
  }
  
  public static navigate(router: Router, analysisId: string) {
    router.navigate([ "/Analysis/Newest", { analysisId: analysisId }]);
  }
  
  public ngOnInit(): any {
    super.ngOnInit();

    var analysisId: string = this.routeParams.get(AnalysisNewestComponent.ROUTE_PARAM_ANALYSIS_ID); 
    var data: remoteData.NewestAnalysisVersionJsonCommand = {
      analysisId: analysisId
    };
    
    this.remoteService.execute(remoteData.NewestAnalysisVersionJsonCommandConstants.NAME, data, {
      data: (dataType: string, data: any) => {
        if (dataType === remoteData.AnalysisVersionJsonResultConstants.TYPE) {
          var d: remoteData.AnalysisVersionJsonResult = <remoteData.AnalysisVersionJsonResult>data;
          
          AnalysisMainComponent.navigate(this.router, analysisId, d.analysisVersion);
        }
        return false;
      },
      exception: (msg: string) => {
      },
      done: () => {}
    });
  }
}

@Component({
  selector: "diqube-analysis-root",
  template: "<router-outlet></router-outlet>",
  directives: [ ROUTER_DIRECTIVES ]
})
@RouteConfig([
  { path: "/", name: "Manage", component: AnalysisManageComponent, useAsDefault: true },
  { path: "/create", name: "Create", component: AnalysisCreateComponent },
  { path: "/:" + AnalysisNewestComponent.ROUTE_PARAM_ANALYSIS_ID, name: "Newest", component: AnalysisNewestComponent },
  { path: "/:" + AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_ID + "/:" + AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_VERSION, name: "Main", component: AnalysisMainComponent }
])
export class AnalysisRootComponent extends DiqubeBaseNavigatableComponent implements OnInit {
  constructor(private loginStateService: LoginStateService) {
    super(true, undefined, loginStateService, undefined);
  }  
}