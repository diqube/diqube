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
import {POLYMER_BINDINGS} from "../../polymer/polymer.bindings";

@Component({
  selector: "diqube-analysis-slice",
  templateUrl: "diqube/analysis/slice/analysis.slice.html",
  directives: [ FORM_DIRECTIVES, POLYMER_BINDINGS ]
})
export class AnalysisSliceComponent implements OnInit {
    
  @Input("slice") public slice: remoteData.UiSlice = undefined;
  
  public nameControl: Control;
  public formControlGroup: ControlGroup;
  
  public editMode: boolean = false;
  public removeMode: boolean = false;
  public normalMode: boolean = true;

  public working: boolean = false;
  
  public sliceEdit: remoteData.UiSlice = undefined;
  
  public exception: string = undefined;
  
  /** true while transitioning (=opening/closing) the iron-collapse */
  public transitioning: boolean = false;
  
  public collapsed: boolean = true;
  
  /**
   * Map from disjunctionIdx/disjunctionValueIdx to an object having a value property which in turn holds the value of
   * sliceEdit.sliceDisjunctions[i].disjunctionValue[j]. The value in this map will be changed by two-way data binding
   * and has to be written back into sliceEdit before updating the slice.
   * 
   * Unfortunately this is needed, since angular2 does not allow us to directly do [(ngModel)]="disjunctionValue[j]",
   * but will then re-create the input field each time the value changes (someone is typing into the field) = the field
   * will lose focus. Therefore we have to bind on [(ngModel)]="something[i].value".
   * 
   * This class keeps structural changes during edit in-sync between disjunctionValueEdit and 
   * sliceEdit.sliceDisjunctions[*].disjuctionValues[*]. Values however are only copied back just before sending the update.  
   */
  public disjunctionValueEdit: {[ disjunctionIdx: number ]: { [disjunctionValueIdx: number]: { value: string } } } = undefined;
  
  constructor(private analysisSateService: AnalysisStateService, private analysisService: AnalysisService, private formBuilder: FormBuilder) {}
  
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
    this.nameControl = new Control("", (control: Control) => {
      return this.nameValidator(control);
    });
    this.formControlGroup = this.formBuilder.group({
      nameControl: this.nameControl,
    });
    
    this.sliceEdit = DiqubeUtil.copy(this.slice);
    this.normalMode = false;
    this.editMode = true;
    this.removeMode = false;
    
    // prepare disjunction values
    this.disjunctionValueEdit = {};
    for (var disjIdx in this.sliceEdit.sliceDisjunctions) {
      var disj = this.sliceEdit.sliceDisjunctions[disjIdx];
      this.disjunctionValueEdit[disjIdx] = {};
      for (var disjValueIdx in disj.disjunctionValues) {
        this.disjunctionValueEdit[disjIdx][disjValueIdx] = { value: disj.disjunctionValues[disjValueIdx] };
      }
    }
    
    this.exception = undefined;
    
    if (this.collapsed)
      // definitely open the collapse element, but do it in next tick, so we get a transition.
      setTimeout(() => {
        this.toggleCollapsed();
      });
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
    this.disjunctionValueEdit = undefined;
    this.normalMode = true;
    this.editMode = false;
    this.removeMode = false;
    
    this.exception = null;
    
    if (!this.collapsed)
      // definitely close the collapse element, but do it in next tick, so we get a transition.
      setTimeout(() => {
        this.toggleCollapsed();
      });
  }
  
  public toggleCollapsed(): void {
    this.transitioning = true;
    this.collapsed = !this.collapsed;
  }
  
  public toggleDone(): void {
    this.transitioning = false;
  }
  
  public removeDisjunctionValue(disjunctionIndex: number, valueIndex: number): void {
    for (var i = valueIndex + 1; i < this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.length; i++) {
      this.disjunctionValueEdit[disjunctionIndex][i - 1] = this.disjunctionValueEdit[disjunctionIndex][i]; 
    }
    delete this.disjunctionValueEdit[disjunctionIndex][this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.length - 1];
    
    this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.splice(valueIndex, 1);
  }
  
  public addDisjunctionValue(disjunctionIndex: number): void {
    this.disjunctionValueEdit[disjunctionIndex][this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.length] = { value: "" };
    this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.push("");
  }

  public removeDisjunctionField(disjunctionIndex: number): void {
    for (var i = disjunctionIndex + 1; i < this.sliceEdit.sliceDisjunctions.length; i++) {
      this.disjunctionValueEdit[i - 1] = this.disjunctionValueEdit[i]; 
    }
    delete this.disjunctionValueEdit[this.sliceEdit.sliceDisjunctions.length - 1];
    
    this.sliceEdit.sliceDisjunctions.splice(disjunctionIndex, 1);
  }

  public addDisjunctionField(fieldName: string): void {
    if (!fieldName)
      return;
    
    this.disjunctionValueEdit[this.sliceEdit.sliceDisjunctions.length] = {};
    this.sliceEdit.sliceDisjunctions.push({
      fieldName: fieldName,
      disjunctionValues: []
    });
  }

  public updateSlice(): void {
    // write values from disjunctionValueEdit back to this.sliceEdit. We can traverse sliceEdit, since all structural
    // changes were kept in-sync.
    for (var disjIdx in this.sliceEdit.sliceDisjunctions) {
      var disj = this.sliceEdit.sliceDisjunctions[disjIdx];
      for (var disjValueIdx in disj.disjunctionValues) {
        disj.disjunctionValues[disjValueIdx] = this.disjunctionValueEdit[disjIdx][disjValueIdx].value; 
      }
    }
    
    this.working = true;
    this.analysisService.updateSlice(this.sliceEdit).then((receivedSlice: remoteData.UiSlice) => {
      this.working = false;
      this.switchToNormalMode();
    }).catch((msg: string) => {
      this.working = false;
      this.exception = msg;
    });
  }
 
  public removeSlice(): void {
    this.analysisService.removeSlice(this.slice.id).catch((msg: string) => {
      this.exception = msg;
    });
    // if successful, slice component will be removed automatically.
  }
  
  public materialElevation(): string {
    return this.collapsed ? "1": "3";
  }
  
  private nameValidator(control: Control): { [key: string]: boolean; }   { 
    if (!control.value || control.value.trim() === "") 
      return { "empty": true };
    return null;
  }
}