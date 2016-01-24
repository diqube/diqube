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
import * as dragData from "../drag-drop/drag-drop.data";
import {DropTargetDirective, DragElementProvider} from "../drag-drop/drop-target.directive";

interface AnalysisSliceDisjunctionValueEditListener {
  valueChanged(el: AnalysisSliceDisjunctionValueEdit): void;
}

class DefaultAnalysisSliceDisjunctionValueEditListener implements AnalysisSliceDisjunctionValueEditListener {
  constructor(private disjunction: remoteData.UiSliceDisjunction, private valueIndex: number) {}
  
  public valueChanged(el: AnalysisSliceDisjunctionValueEdit): void {
    this.disjunction.disjunctionValues[this.valueIndex] = el.getValue();
  }
}

/**
 * Helper class that provides a "value" property which will fire events after the field value has changed.
 */
export class AnalysisSliceDisjunctionValueEdit {
  private internalValue: string = undefined;
  private disableListener: boolean = false;
  
  constructor(startValue: string, private listener: AnalysisSliceDisjunctionValueEditListener) {
    this.internalValue = startValue;
    
    var me = this;
    
    Object.defineProperty(this, "value", {
      get: () => { 
        return me.internalValue;  
      },
      set: (value: string) => { 
        me.internalValue = value;
        if (!me.disableListener)
          listener.valueChanged(me); 
      },
      enumerable: true
    });
  }
  
  public setValueWithoutListener(value: string) {
    this.disableListener = true;
    (<any>this).value = value;
    this.disableListener = false;
  }
  
  public getValue(): string {
    return this.internalValue;
  }
}

@Component({
  selector: "diqube-analysis-slice",
  templateUrl: "diqube/analysis/slice/analysis.slice.html",
  directives: [ FORM_DIRECTIVES, POLYMER_BINDINGS, DropTargetDirective ]
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
  
  /** 
   * When currently transitioning (= collapsing or un-collapsing) this field holds a pointer to the resolve-function 
   * that was returned by toggleCollapse() and which will be called as soon as the trnasition finishes 
   */
  public currentTransitioningPromiseResolve: (a: void) => void = undefined;
  
  public collapsed: boolean = true;
  public addCollapsedToDom: boolean = false;
  
  /**
   * Map from disjunctionIdx/disjunctionValueIdx to an object having a value property which in turn holds the value of
   * sliceEdit.sliceDisjunctions[i].disjunctionValue[j]. The value in this map will be changed by two-way data binding
   * and has to be written back into sliceEdit, which is done by listeners on the AnalysisSliceDisjunctionValueEdit objects.
   * 
   * Unfortunately this is needed, since angular2 does not allow us to directly do [(ngModel)]="disjunctionValue[j]",
   * but will then re-create the input field each time the value changes (someone is typing into the field) = the field
   * will lose focus. Therefore we have to bind on [(ngModel)]="something[i].value".
   * 
   * DO NOT access directly, but using property "disjunctionValueEdit" which will effectively update the values 
   * dynamically on each call.
   */
  private internalDisjunctionValueEdit: Array<Array<AnalysisSliceDisjunctionValueEdit>>;
  
  constructor(private analysisSateService: AnalysisStateService, private analysisService: AnalysisService, 
              private formBuilder: FormBuilder) {
    var me = this;
    // give angular a nice property to bind to, so it does not know that we actually do a method call here.
    Object.defineProperty(this, "disjunctionValueEdit", {
      get: () => {
        return me.internalRefreshAndGetDisjunctionValueEdit();
      }
    });
  }
  
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
    if (this.transitioning())
      return;

    var closePromise: Promise<void>;
    
    if (this.collapsed) {
      // closed already.
      closePromise = Promise.resolve(undefined);
    } else {
      // opened currently. Close first, then open again later.
      closePromise = new Promise((resolve: (a: void) => void, reject: (a: void) => void) => {
        setTimeout(() => {
          this.toggleCollapsed().then(() => { 
            resolve(undefined); 
         });
        });
      });
    }

    // after closed, switch to edit mode.
    closePromise.then(() => {
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
      
      this.internalDisjunctionValueEdit = [];
      
      this.exception = undefined;
      
      // toggle open (again)
      setTimeout(() => {
        this.toggleCollapsed();
      });
    });
  }

  /**
   * Switch to remove mode.
   */
  public switchToRemoveMode(): void {
    if (this.transitioning())
      return;
    
    var closePromise: Promise<void>;
    if (this.collapsed) {
      closePromise = Promise.resolve(undefined);
    } else {
      // collapse (in setTimeout to get a transition), then switch to remove mode.
      closePromise = new Promise((resolve: (a: void) => void, reject: (a: void) => void) => {
        setTimeout(() => { this.toggleCollapsed().then(() => {
            resolve(undefined);
          })
        });
      });
    }
    
    closePromise.then(() => {
      this.normalMode = false;
      this.editMode = false;
      this.removeMode = true;
      this.exception = undefined;
    });
  }
  
  /**
   * Switch to normal mode.
   */
  public switchToNormalMode(): void {
    if (this.transitioning())
      return;
    
    var closePromise: Promise<void>;
    if (this.collapsed) {
      // closed already
      closePromise = Promise.resolve(undefined);
    } else {
      // collapse (in setTimeout to get a transition), then switch to normal mode.
      closePromise = new Promise((resolve: (a: void) => void, reject: (a: void) => void) => {
        setTimeout(() => { this.toggleCollapsed().then(() => {
            resolve(undefined);
          })
        });
      });
    }
    
    closePromise.then(() => {
      this.sliceEdit = undefined;
      this.internalDisjunctionValueEdit = undefined;
      this.normalMode = true;
      this.editMode = false;
      this.removeMode = false;
      this.exception = null;
    });
  }
  
  public toggleCollapsed(): Promise<void> {
    if (this.transitioning())
      return Promise.reject(undefined);
    
    if (this.collapsed) {
      // switching to "open"
      return new Promise((resolve: (a: void) => void, reject: (a: void) => void) => {
        this.currentTransitioningPromiseResolve = resolve;
        this.addCollapsedToDom = true;
        setTimeout(() => {
          this.collapsed = false;
        });
      });
    } else {
      // switching to "closed"
      return new Promise((resolve: (a: void) => void, reject: (a: void) => void) => {
        this.currentTransitioningPromiseResolve = resolve;
        this.collapsed = true;
        // addCollpasedToDom is changed by toggleDone
      });
    }
  }
  
  public toggleDone(): void {
    if (!this.transitioning())
      return;
    
    if (this.collapsed) {
      // remove stuff from DOM.
      this.addCollapsedToDom = false;
    }
    
    var resolve: (a: void) => void = this.currentTransitioningPromiseResolve;
    // "complete" transitioning. Do this before resolving the promise, as the promise might want to start another toggle right away. 
    this.currentTransitioningPromiseResolve = undefined;
    resolve(undefined);
  }
  
  public removeDisjunctionValue(disjunctionIndex: number, valueIndex: number): void {
    this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.splice(valueIndex, 1);
    this.internalRefreshAndGetDisjunctionValueEdit();
  }
  
  public addDisjunctionValue(disjunctionIndex: number): void {
    this.addDisjunctionValueInternal(disjunctionIndex, "");
  }
  
  private addDisjunctionValueInternal(disjunctionIndex: number, value: string): void {
    this.sliceEdit.sliceDisjunctions[disjunctionIndex].disjunctionValues.push(value);
    this.internalRefreshAndGetDisjunctionValueEdit();
  }

  public removeDisjunctionField(disjunctionIndex: number): void {
    this.sliceEdit.sliceDisjunctions.splice(disjunctionIndex, 1);
    this.internalRefreshAndGetDisjunctionValueEdit();
  }

  public addDisjunctionField(fieldName: string): void {
    this.addDisjunctionFieldInternal(fieldName, undefined);
  }
  
  private addDisjunctionFieldInternal(fieldName: string, disjunctionValue: string): void {
    if (!fieldName)
      return;
    
    var values: Array<string> = [];
    if (disjunctionValue)
      values.push(disjunctionValue);
    
    this.sliceEdit.sliceDisjunctions.push({
      fieldName: fieldName,
      disjunctionValues: values
    });
    this.internalRefreshAndGetDisjunctionValueEdit();
  }

  public updateSlice(): void {
    this.sendUpdatedSlice(this.sliceEdit);
  }
  
  private sendUpdatedSlice(slice: remoteData.UiSlice): void {
    this.working = true;
    this.analysisService.updateSlice(slice).then((receivedSlice: remoteData.UiSlice) => {
      this.working = false;
      if (!this.normalMode)
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
  
  public drop(elementProvider: DragElementProvider): void {
    var restriction: dragData.DragDropRestrictionData = <dragData.DragDropRestrictionData>elementProvider.element().data;
    
    if (this.normalMode) {
      var sliceToEdit: remoteData.UiSlice = DiqubeUtil.copy(this.slice);
      
      var availableDisjunctions: Array<remoteData.UiSliceDisjunction> = 
        sliceToEdit.sliceDisjunctions.filter(function (d) { return d.fieldName === restriction.field });
    
      if (availableDisjunctions && availableDisjunctions.length) {
        availableDisjunctions[0].disjunctionValues.push(restriction.value);
      } else {
        sliceToEdit.sliceDisjunctions.push({
          fieldName: restriction.field,
          disjunctionValues: [ restriction.value ]
        });
      }
      this.sendUpdatedSlice(sliceToEdit);
      elementProvider.handled();
    } else if (this.editMode) {
      var disjIdx: number = 
        this.sliceEdit.sliceDisjunctions.findIndex(function (d) { return d.fieldName === restriction.field });
    
      if (disjIdx >= 0) {
        this.addDisjunctionValueInternal(disjIdx, restriction.value);
      } else {
        this.addDisjunctionFieldInternal(restriction.field, restriction.value);
      }
      elementProvider.handled();
    }
  }
  
  /**
   * Recalculate this.internalDisjunctionValueEdit according to current values in this.sliceEdit and return this.internalDisjunctionValueEdit.
   * 
   * Does try to not overwrite any values that are still valid to not distract angular and lead it to re-create any 
   * view-components although it does not need to.
   */
  public internalRefreshAndGetDisjunctionValueEdit(): Array<Array<AnalysisSliceDisjunctionValueEdit>> {
    for (var disjIdx in this.sliceEdit.sliceDisjunctions) {
      var disj: remoteData.UiSliceDisjunction = this.sliceEdit.sliceDisjunctions[disjIdx];
      if (this.internalDisjunctionValueEdit.length <= disjIdx) {
        // disjunctionValueEdit has too few elements
        this.internalDisjunctionValueEdit.push([]);
      }
      
      for (var valueIdx in disj.disjunctionValues) {
        if (this.internalDisjunctionValueEdit[disjIdx].length <= valueIdx) {
          // disjunctionValueEdit[disjIdx] has too few elements, create a new one with a onchange-listener 
          var targetIdx: number = parseInt(valueIdx);
          this.internalDisjunctionValueEdit[disjIdx].push(
            new AnalysisSliceDisjunctionValueEdit(disj.disjunctionValues[targetIdx], 
              new DefaultAnalysisSliceDisjunctionValueEditListener(disj, targetIdx)));
        } else {
          // check if disjunctionValueEdit[disjIdx][valueIdx] still has correct value or if it was changed in the sliceEdit object! 
          if (this.internalDisjunctionValueEdit[disjIdx][valueIdx].getValue() !== disj.disjunctionValues[valueIdx]) {
            this.internalDisjunctionValueEdit[disjIdx][valueIdx].setValueWithoutListener(disj.disjunctionValues[valueIdx]);
          }
        }
      }
      
      if (disj.disjunctionValues.length < this.internalDisjunctionValueEdit[disjIdx].length) {
        // remove elements in djusjunctionValueEdit that have been removed in sliceEdit
        this.internalDisjunctionValueEdit[disjIdx].splice(disj.disjunctionValues.length, this.internalDisjunctionValueEdit[disjIdx].length - disj.disjunctionValues.length);
      }
    }

    if (this.sliceEdit.sliceDisjunctions.length < this.internalDisjunctionValueEdit.length) {
      // remove elements in djusjunctionValueEdit that have been removed in sliceEdit
      this.internalDisjunctionValueEdit.splice(this.sliceEdit.sliceDisjunctions.length, this.internalDisjunctionValueEdit.length - this.sliceEdit.sliceDisjunctions.length);
    }
    
    return this.internalDisjunctionValueEdit;
  }
  
  /**
   * returns true if currently transitioning, i.e. if collapsing or un-collapsing.
   */
  public transitioning(): boolean {
    return this.currentTransitioningPromiseResolve !== undefined;
  }
}