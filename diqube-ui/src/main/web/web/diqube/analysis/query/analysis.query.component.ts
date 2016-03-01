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

import {Component, OnInit, Input} from "angular2/core";
import {Control, ControlGroup, FormBuilder, FORM_DIRECTIVES} from "angular2/common";
import {AnalysisService} from "../analysis.service";
import {AnalysisStateService} from "../state/analysis.state.service";
import * as remoteData from "../../remote/remote";
import * as analysisData from "../analysis";
import {DiqubeUtil} from "../../util/diqube.util";
import {AnalysisQueryBarchartComponent} from "./analysis.query.barchart.component";
import {AnalysisQueryTableComponent} from "./analysis.query.table.component";
import {POLYMER_BINDINGS} from "../../polymer/polymer.bindings";

@Component({
  selector: "diqube-analysis-query",
  templateUrl: "diqube/analysis/query/analysis.query.html",
  directives: [ FORM_DIRECTIVES, AnalysisQueryBarchartComponent, POLYMER_BINDINGS, AnalysisQueryTableComponent ]
})
export class AnalysisQueryComponent implements OnInit {

  public static VALID_QUERY_DISPLAY_TYPES : Array<{id: string, title: string}> = [ 
    { 
      id: remoteData.UiQueryConstants.DISPLAY_TYPE_TABLE,
      title: "Table"
    }, {
      id: remoteData.UiQueryConstants.DISPLAY_TYPE_BARCHART,
      title: "Bar Chart"
    } ];
  
  @Input("qube") public qube: remoteData.UiQube = undefined;
  @Input("query") public query: remoteData.UiQuery = undefined;

  public editMode: boolean = false;
  public removeMode: boolean = false;
  public normalMode: boolean = true;

  public working: boolean = false;

  public nameControl: Control;
  public diqlControl: Control;
  public formControlGroup: ControlGroup;

  public queryEdit: remoteData.UiQuery = undefined;
  
  private exceptionOverride: string = undefined;
  private ignoreResultsException: boolean = false;
  
  constructor(private analysisService: AnalysisService, private formBuilder: FormBuilder, private analysisSateService: AnalysisStateService) {}
  
  public ngOnInit(): any {
    if (this.analysisSateService.pollOpenQueryInEditModeNextTime(this.query.id))
      this.switchToEditMode();
    else
      this.switchToNormalMode();
  }
  
  /**
   * The currently valid exception
   */
  public exception(): string {
    if (this.exceptionOverride)
      return this.exceptionOverride;
    
    if (!this.ignoreResultsException && analysisData.isUiQueryWithResults(this.query))
      return (<analysisData.UiQueryWithResults>this.query).$results.exception;
    
    return undefined;
  }
  
  /**
   * Provide a results object of the query, request new execution if needed.
   */
  public queryResults(): analysisData.EnhancedTableJsonResult {
    if (analysisData.isUiQueryWithResults(this.query))
      return (<analysisData.UiQueryWithResults>this.query).$results;
    
    this.working = true;
    this.analysisService.provideQueryResults(this.qube, this.query, undefined).then((res: analysisData.EnhancedTableJsonResult) => {
      this.working = false;
    }).catch((res: analysisData.EnhancedTableJsonResult) => {
      this.working = false;
      // exception is available in query.$results.exception 
    });
    
    // query has been extended to a UiQueryWithResults now.
    return (<analysisData.UiQueryWithResults>this.query).$results;
  }
  
  /**
   * Switch the display type of the query with one click (without switching to edit mode)
   */
  public switchQueryDisplayType(displayTypeId: string): void {
    var newQuery: remoteData.UiQuery = DiqubeUtil.copy(this.query);
    newQuery.displayType = displayTypeId;
    
    this.working = true;
    this.analysisService.updateQuery(this.qube.id, newQuery).then(() => {
      this.working = false;
    }).catch((msg: string) => {
      this.working = false; 
      this.exceptionOverride = msg; 
    });
  }

  /**
   * Get a full object of details about a specific display type.
   */
  public getDisplayTypeOptions(dispalyTypeId: string): {id: string, title: string} {
    return AnalysisQueryComponent.VALID_QUERY_DISPLAY_TYPES.filter((t) => { return t.id === dispalyTypeId; })[0];
  }
  
  public getAllDisplayTypeOptions(): Array<{id: string, title: string}> {
    return AnalysisQueryComponent.VALID_QUERY_DISPLAY_TYPES;
  }

  /**
   * Switch to edit mode.
   */
  public switchToEditMode(): void {
    this.nameControl = new Control("", (control: Control) => {
      return this.nameValidator(control);
    });
    this.diqlControl = new Control("", (control: Control) => {
      return this.diqlValidator(control);
    });
    
    this.formControlGroup = this.formBuilder.group({
      nameControl: this.nameControl,
      diqlControl: this.diqlControl
    });
    
    this.queryEdit = DiqubeUtil.copy(this.query);
    this.normalMode = false;
    this.editMode = true;
    this.removeMode = false;
    this.ignoreResultsException = true;
    
    this.exceptionOverride = undefined;

    // TODO resize textarea
//    setTimeout(() => {
//      var textarea = $("textarea", element);
//      // resize text area so it does not need to be scrolled from the beginning...
//      textarea.height(textarea[0].scrollHeight + 10);
//      var newWidth = textarea[0].scrollWidth + 10;
//      if (newWidth > window.innerWidth / 2)
//        // let width at max be half of the window size. User can resize manually and has scrolling...
//        newWidth = window.innerWidth / 2;
//      textarea.width(newWidth);
//    }, 0);
  }

  /**
   * Switch to remove mode.
   */
  public switchToRemoveMode(): void {
    this.normalMode = false;
    this.editMode = false;
    this.removeMode = true;
    this.ignoreResultsException = true;

    this.exceptionOverride = undefined;
  }
  
  /**
   * Switch to normal mode.
   */
  public switchToNormalMode(): void {
    this.queryEdit = undefined;
    this.normalMode = true;
    this.editMode = false;
    this.removeMode = false;
    this.ignoreResultsException = false;
    
    this.exceptionOverride = null;
  }

  public updateQuery(): void {
    this.exceptionOverride = undefined;
    
    this.working = true;
    this.analysisService.updateQuery(this.qube.id, this.queryEdit).then((receivedQuery: remoteData.UiQuery) => {
      this.working = false;
      this.switchToNormalMode();
    }).catch((msg: string) => {
      this.working = false;
      this.exceptionOverride = msg;
    });
  }

  public removeQuery(): void {
    this.exceptionOverride = undefined;
    
    this.working = true;
    this.analysisService.removeQuery(this.qube.id, this.query.id).then((a: void) => {
      this.working = false;
      // this component will be removed automatically, since the parent qube and the parent analysis controller
      // will update automatically.
    }).catch((msg: string) => {
      this.working = false;
      this.exceptionOverride = msg;
      this.switchToNormalMode();
    });
  }
  
  private nameValidator(control: Control): { [key: string]: boolean; }   { 
    if (!control.value || control.value.trim() === "") 
      return { "empty": true };
    return null;
  }
  
  private diqlValidator(control: Control): { [key: string]: boolean; }   { 
    if (!control.value || control.value.trim() === "") 
      return { "empty": true };
    return null;
  }

}