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

import {Component, OnInit, OnDestroy, Inject} from "angular2/core";
import {RouteParams, Router, ROUTER_DIRECTIVES, CanReuse, OnReuse, ComponentInstruction} from "angular2/router";
import {AnalysisService, AnalysisServiceRenavigator} from "./analysis.service";
import * as remoteData from "../remote/remote";
import {LoginStateService} from "../login-state/login-state.service";
import {AnalysisQubeComponent} from "./qube/analysis.qube.component";
import {AnalysisSlicesComponent} from "./slice/analysis.slices.component";
import {NavigationStateService} from "../navigation-state/navigation-state.service";
import {DiqubeBaseNavigatableComponent} from "../diqube.base.component";
import {DragControlDirective} from "./drag-drop/drag-control.directive";

/**
 * Main component for displaying a specific version of an analysis.
 * 
 * This component will be re-used if the user switches between different versions of the same analysis, which happens
 * frequently when using the forward/back buttons in the browser. We do not want to re-create everything in that case,
 * since everything should be fairly similar. The component will be re-created fully though when navigating to a
 * different analysis.
 * 
 * This object serves as AnalysisServiceRenavigator for the analysis service as this component is active always
 * when the currently loaded analysis is changed!
 */
@Component({
  selector: "diqube-analysis-main",
  templateUrl: "diqube/analysis/analysis.main.html",
  directives: [ ROUTER_DIRECTIVES, AnalysisQubeComponent, AnalysisSlicesComponent, DragControlDirective ]
})
export class AnalysisMainComponent extends DiqubeBaseNavigatableComponent implements OnInit, OnDestroy , CanReuse, OnReuse, AnalysisServiceRenavigator {
  public static ROUTE_PARAM_ANALYSIS_ID: string = "analysisId";
  public static ROUTE_PARAM_ANALYSIS_VERSION: string = "analysisVersion";
  
  public analysis: remoteData.UiAnalysis;
  private paramAnalysisId: string;
  private paramAnalysisVersion: number;
  
  public error: string = "";
  
  constructor(private analysisService: AnalysisService, private routeParams: RouteParams, 
              private loginStateService: LoginStateService, private router: Router, private navigationStateService: NavigationStateService) {
    super(true, "Analysis", loginStateService, navigationStateService);
  }
  
  public static navigate(router: Router, analysisId: string, analysisVersion: number) {
    router.navigate([ "/Analysis/Main", { analysisId: analysisId, analysisVersion: analysisVersion }]);
  }

  public analysisServiceReloadNeeded(analysisId: string, analysisVersion: number): boolean {
    // do not re-navigate if we're there already. This might happen if we're reusing this component because of a 
    // back/forward navigation of the user: The paramAnalysisVersion is set first in the URL (by the browser), then
    // paramAnalysisVersion is set and then this event fires (as analysis service did reload the analysis from the
    // server) - but in that case we are already at the right location in the browser and must no re-navigate as that
    // would destroy the browser history (after a back we would push the now-newest URL again right away, deleting the 
    // original forward-history).
    if (this.paramAnalysisVersion !== analysisVersion) {
      AnalysisMainComponent.navigate(this.router, analysisId, analysisVersion);
    }
    return false; // navigation successful, do not propagate further
  }
  
  public ngOnInit(): any {
    super.ngOnInit();
    
    this.analysisService.registerRenvaigator(this);
    this.paramAnalysisId = this.routeParams.get(AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_ID);
    this.loadNewAnalysisVersion(this.routeParams.get(AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_VERSION));
  }
  
  public ngOnDestroy(): any {
    this.analysisService.unregisterRenavigator(this);
    this.analysisService.unloadAnalysis();
  }
  
  /**
   * Fully loads a new version of the analysis identified by this.paramAnalysisId.
   */
  private loadNewAnalysisVersion(analysisVersion: string): void {
    this.paramAnalysisVersion = parseInt(analysisVersion);

    var me: AnalysisMainComponent = this;
    this.analysisService.loadAnalysis(this.paramAnalysisId, this.paramAnalysisVersion).then((a: remoteData.UiAnalysis) => {
      me.loadAnalysis(a);
    }).catch((msg: string) => { 
      me.loadAnalysis(undefined);
      me.error = msg; 
    });
  }
  
  /**
   * Sets all properties of a fully loaded UiAnalysis that should be displayed.
   */
  private loadAnalysis(analysis: remoteData.UiAnalysis): void {
    if (!analysis) {
      this.analysis = undefined;
      this.error = undefined;
      this.navigationStateService.setCurrentTitle("Analysis " + this.paramAnalysisId);
      return;
    }
    this.analysis = analysis;
    this.error = undefined;
    if (this.analysis.name)
      this.navigationStateService.setCurrentTitle("Analysis " + this.analysis.name + " (" + this.analysis.table + ")");
    else
      this.navigationStateService.setCurrentTitle("Analysis " + this.analysis.id + " (" + this.analysis.table + ")");
  }
  
  public showNewerVersionWarning(): boolean {
    return this.analysis && 
           this.analysis.version < this.analysisService.newestVersionOfAnalysis &&
           this.isWritable();
  }
  
  public newestVersionNumber(): number {
    return this.analysisService.newestVersionOfAnalysis;
  }
  
  public showDifferentOwnerWarning(): boolean {
    return this.analysis && !this.isWritable();
  }
  
  private isWritable(): boolean {
    return this.analysis &&
           // only if we're logged in with the same user that owns the analysis, we will be able to execute any
           // remote calls that change the analysis.
           this.loginStateService.username === this.analysis.user;
  }
  
  public addQube(): void {
    var slicePromise: Promise<remoteData.UiSlice>;
    if (this.analysis.slices.length == 0)
      slicePromise = this.analysisService.addSlice("Default slice", "", []);
    else {
      slicePromise = Promise.resolve<remoteData.UiSlice>(this.analysis.slices[0]); 
    }
    
    slicePromise.then((slice: remoteData.UiSlice) => {
      this.analysisService.addQube("New qube", slice.id);
    }).catch((text: string) => {
      // TODO nicer error?
      this.error = text;
    });
  }
  
  public cloneAndLoadCurrentAnalysis(): void {
    this.analysisService.cloneAndLoadCurrentAnalysis();
  }
  
  public loadNewestVersion(): void {
    AnalysisMainComponent.navigate(this.router, this.analysis.id, this.newestVersionNumber());
  }
 
  
  public routerCanReuse(nextInstruction: ComponentInstruction, prevInstruction: ComponentInstruction): any {
    // only reuse this component if we're navigating through the same analysis. Re-create component otherwise.
    return nextInstruction.params[AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_ID] == prevInstruction.params[AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_ID];
  }
  
  public routerOnReuse(nextInstruction: ComponentInstruction, prevInstruction: ComponentInstruction): any {
    this.loadNewAnalysisVersion(nextInstruction.params[AnalysisMainComponent.ROUTE_PARAM_ANALYSIS_VERSION]);
  }
}