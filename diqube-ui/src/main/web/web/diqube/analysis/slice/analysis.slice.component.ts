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

@Component({
  selector: "diqube-analysis-slice",
  templateUrl: "diqube/analysis/slice/analysis.slice.html",
  directives: [ FORM_DIRECTIVES ]
})
export class AnalysisSliceComponent implements OnInit {
    
  @Input("slice") public slice: remoteData.UiSlice = undefined;
  @Input("additionalClass") public additionalClass: string = undefined;
  
  public editMode: boolean = false;
  public removeMode: boolean = false;
  public normalMode: boolean = true;

  public working: boolean = false;
  
  public sliceEdit: remoteData.UiSlice = undefined;
  
  public exception: string = undefined;
  public editException: string = undefined;
  public collapsed: boolean = false;
  
  constructor(private analysisSateService: AnalysisStateService, private analysisService: AnalysisService) {}
  
  public ngOnInit(): any {
    if (this.analysisSateService.pollOpenSliceInEditModeNextTime(this.slice.id))
      this.switchToEditMode();
    else
      this.switchToNormalMode();
  }
  
  /**
   * Switch to edit mode.
   */
  public switchToEditMode(): void {
//    this.nameControl = new Control("", (control: Control) => {
//      return this.nameValidator(control);
//    });
//    this.diqlControl = new Control("", (control: Control) => {
//      return this.diqlValidator(control);
//    });
//    
//    this.formControlGroup = this.formBuilder.group({
//      nameControl: this.nameControl,
//      diqlControl: this.diqlControl
//    });
    
    this.sliceEdit = DiqubeUtil.copy(this.slice);
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
  
  /**
   * Switch to normal mode.
   */
  public switchToNormalMode(): void {
    this.sliceEdit = undefined;
    this.normalMode = true;
    this.editMode = false;
    this.removeMode = false;
    
    this.exception = null;
  }
  
  public toggleCollapsed(): void {
    this.collapsed = !this.collapsed;
  }
  
  public removeDisjunctionValue(disjunctionIndex: number, valueIndex: number): void {
    this.slice.sliceDisjunctions[disjunctionIndex].disjunctionValues.splice(valueIndex, 1);
  }
  
  public addDisjunctionValue(disjunctionIndex: number): void {
    this.slice.sliceDisjunctions[disjunctionIndex].disjunctionValues.push("");
  }

  public removeDisjunctionField(disjunctionIndex: number): void {
    this.slice.sliceDisjunctions.splice(disjunctionIndex, 1);
  }

  public addDisjunctionField(fieldName: string): void {
    if (!fieldName)
      return;
    
    this.slice.sliceDisjunctions.push({
      fieldName: fieldName,
      disjunctionValues: []
    });
  }

  public updateSlice(): void {
    this.working = true;
    this.analysisService.updateSlice(this.sliceEdit).then((receivedSlice: remoteData.UiSlice) => {
      this.working = false;
      this.switchToNormalMode();
    }).catch((msg: string) => {
      this.working = false;
      this.editException = msg;
    });
  }
 
  public removeSlice(): void {
    this.analysisService.removeSlice(this.slice.id).catch((msg: string) => {
      this.exception = msg;
    });
    // if successful, slice component will be removed automatically.
  }
}