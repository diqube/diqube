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
import * as remoteData from "../../remote/remote";
import * as analysisData from "../analysis";
import {DiqubeUtil} from "../../util/diqube.util";
import {AnalysisQueryComponent} from "../query/analysis.query.component";
import {POLYMER_BINDINGS} from "../../polymer/polymer.bindings";

@Component({
  selector: "diqube-analysis-qube",
  templateUrl: "diqube/analysis/qube/analysis.qube.html",
  directives: [ FORM_DIRECTIVES, POLYMER_BINDINGS, AnalysisQueryComponent ]
})
export class AnalysisQubeComponent {

  @Input("analysis") public analysis: remoteData.UiAnalysis = undefined;
  @Input("qube") public qube: remoteData.UiQube = undefined;
  
  public editMode: boolean = false;
  public removeMode: boolean = false;
  public normalMode: boolean = true;

  public nameControl: Control;
  public formControlGroup: ControlGroup;
  
  public qubeEdit: remoteData.UiQube = undefined;
  
  public exception: string = undefined;
  private sliceCache: remoteData.UiSlice = undefined;
  
  public working: boolean = false;

  constructor(private analysisService: AnalysisService, private formBuilder: FormBuilder) {}

  /**
   * The UiSlice object corresponding to the slice this qube belongs to.
   */
  public slice(): remoteData.UiSlice {
    if (!this.sliceCache || this.sliceCache.id !== this.qube.sliceId)
      this.sliceCache = this.analysis.slices.filter((s) => { return s.id === this.qube.sliceId; })[0]; 
    return this.sliceCache;
  } 
  
  /**
   * Switch to edit mode.
   */
  public switchToEditMode(): void {
    this.nameControl = new Control("", (control: Control) => {
      return this.nameValidator(control);
    });
    
    this.formControlGroup = this.formBuilder.group({
      nameControl: this.nameControl,
    });
    
    this.qubeEdit = DiqubeUtil.copy(this.qube);
    this.normalMode = false;
    this.editMode = true;
    this.removeMode = false;
    
    this.exception = undefined;
  }

  /**
   * Switch to remove mode.
   */
  public switchToRemoveMode(): void {
    this.normalMode = false;
    this.editMode = false;
    this.removeMode = true;

    this.exception = undefined;
  }
  
  public updateQube(): void {
    this.working = true;
    this.analysisService.updateQube(this.qubeEdit).then((newQube: remoteData.UiQube) => {
      this.working = false;
      this.switchToNormalMode();
    }).catch((text: string) => {
      this.working = false;
      this.exception = text;
    })
  }
  
  public removeQube(): void {
    this.working = true;
    this.analysisService.removeQube(this.qube.id).then((a: void) => {
      // qube component will be removed automatically.
      this.working = false;
    }).catch((msg: string) => {
      this.working = false;
      this.switchToNormalMode();
    });
  }
  
  public addQuery(): void {
    this.analysisService.addQuery("New query", "", this.qube.id).catch((msg: string) => {
      this.exception = msg;
    });
  }

  /**
   * Switch to normal mode.
   */
  public switchToNormalMode(): void {
    this.qubeEdit = undefined;
    this.normalMode = true;
    this.editMode = false;
    this.removeMode = false;
    
    this.exception = undefined;
  }
  
  private nameValidator(control: Control): { [key: string]: boolean; } {
    if (!control.value || control.value.trim() === "") 
      return { "empty": true };
    return null;
  }
  
}