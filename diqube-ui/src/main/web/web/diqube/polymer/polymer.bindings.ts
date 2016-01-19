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

import {Directive, ElementRef, Renderer, Self, forwardRef, Provider, Optional, Injector, Injectable, OnInit, Input} from "angular2/core";
import {ControlValueAccessor, NgModel, NgControl, NG_VALUE_ACCESSOR, NgForm} from "angular2/common";

const POLYMER_INPUT_VALUE_ACCESSOR = 
  new Provider(NG_VALUE_ACCESSOR, {useExisting: forwardRef(() => PolymerInputValueAccessor), multi: true});

const POLYMER_CHECKBOX_VALUE_ACCESSOR = 
  new Provider(NG_VALUE_ACCESSOR, {useExisting: forwardRef(() => PolymerCheckboxValueAccessor), multi: true});

const POLYMER_DROPDOWN_VALUE_ACCESSOR = 
  new Provider(NG_VALUE_ACCESSOR, {useExisting: forwardRef(() => PolymerDropdownValueAccessor), multi: true});

/**
 * All polymer bindings.
 */
export const POLYMER_BINDINGS = [ 
  forwardRef(() => PolymerInputValueAccessor), 
  forwardRef(() => PolymerCheckboxValueAccessor), 
  forwardRef(() => PolymerDropdownValueAccessor) ];

/**
 * A ControlValueAccessor that binds to paper-input and paper-textarea. In addition to writing the values to the element,
 * this class also evaluates errors reported on the NgControl and displays them on the paper-input/paper-textarea.
 * 
 * In addition to the above, this directive manually identifies the <ENTER> keypress on a paper-input and submits the first
 * parent form that is found (if any).
 */
@Directive({
  selector: "paper-input[ngModel],paper-input[ngControl],paper-textarea[ngModel],paper-textarea[ngControl]",
  host: {
    "(input)": "onChange($event.target.value)", 
    "(blur)": "onTouched()",
    "(keypress)": "onKeypress($event)"
  },
  // load POLYMER_VALUE_ACCESSOR into DI as soon as PolymerValueAccessor is loaded -> That will in turn register 
  // PolymerValueAccessor as an object belonging to NG_VALUE_ACCESSOR and will then transparently be found by ngModel
  // and ngControl etc.
  bindings: [ POLYMER_INPUT_VALUE_ACCESSOR ]
})
export class PolymerInputValueAccessor implements ControlValueAccessor, OnInit {
  private delegateOnChange = (_: any) => {};
  private delegateOnTouched = () => {};
  private cachedPolymerInjectable: PolymerInjectable = undefined;
  private doAutomaticFormSubmit: boolean;
  
  @Input("errorMessages") public errorMessages: { [key: string]: string }; 
  
  constructor(private _renderer: Renderer, private _elementRef: ElementRef, private injector: Injector) {
    var tagName: string = (<HTMLElement>_elementRef.nativeElement).tagName.toLowerCase();
    if (tagName === "paper-input")
      this.doAutomaticFormSubmit = true;
    else
      // no automatic submit in textareas!
      this.doAutomaticFormSubmit = false;
  }

  public ngOnInit(): any {
    this.adjustValidityState();
  }
  
  public writeValue(value: any): void {
    var normalizedValue: string;
    if (!value || (value.trim && value.trim() === ""))
      normalizedValue = "";
    else
      normalizedValue = value;
    
    (<HTMLElement>this._elementRef.nativeElement).setAttribute("value", value);
  }

  public registerOnChange(fn: (_: any) => void): void { 
    this.delegateOnChange = fn; 
  }
  
  public registerOnTouched(fn: () => void): void { 
    this.delegateOnTouched = fn; 
  }
  
  private onChange(newValue: any): void {
    this.delegateOnChange(newValue);
    this.adjustValidityState();
  }
  
  private onTouched(newValue: any): void {
    this.delegateOnTouched();
    this.adjustValidityState();
  }
  
  private onKeypress(event: KeyboardEvent): void {
    if (this.doAutomaticFormSubmit) {
      var keyCode = event.charCode || event.keyCode;
      if (keyCode === 13) {
        // <ENTER>
        var form: NgForm = this.directiveInfo().ngForm;
        if (form)
          form.onSubmit();
      }
    }
  }
  
  private adjustValidityState(): void {
    if (this.directiveInfo().ngControl) {
      var isValid: boolean = !this.directiveInfo().ngControl.dirty || this.directiveInfo().ngControl.valid; 
      this.applyValidity(isValid);
      
      if (!isValid) {
        var errorList: Array<string> = [];
        var errors: { [key:string]: any } = this.directiveInfo().ngControl.errors;
        for (var key in errors) {
          if (errors.hasOwnProperty(key)) {
            errorList.push(key);
          }
        }
        errorList.sort();

        var error: string = errorList[0];
        if (this.errorMessages)
          error = this.errorMessages[error];
        
        this.applyErrorMessage(error);
      }
    } else
      this.applyValidity(true);
  }
  
  private applyValidity(isValid: boolean): void {
    if (isValid)
      (<HTMLElement>this._elementRef.nativeElement).removeAttribute("invalid");
    else {
     (<HTMLElement>this._elementRef.nativeElement).setAttribute("invalid", "true");
    }
  }
  
  private applyErrorMessage(error: string): void {
    (<HTMLElement>this._elementRef.nativeElement).setAttribute("error-message", error);
  }
  
  private directiveInfo(): PolymerInjectable {
    if (!this.cachedPolymerInjectable)
      this.cachedPolymerInjectable = this.injector.resolveAndInstantiate(PolymerInjectable);
    
    return this.cachedPolymerInjectable;
  }
}

/**
 * Helper class that can be instantiated using the injector and which will then provide the corresponding ngControl/etc where the directive is active.
 */
@Injectable()
class PolymerInjectable {
  constructor(@Self() @Optional() public ngControl: NgControl, @Optional() public ngForm: NgForm) {} 
}

/**
 * Analogous to PolymerInputValueAccessor, but for a paper-checkbox.
 */
@Directive({
  selector: "paper-checkbox[ngModel],paper-checkbox[ngControl]",
  host: {
    "(iron-change)": "onChange($event.target.checked)"
  },
  bindings: [ POLYMER_CHECKBOX_VALUE_ACCESSOR ]
})
export class PolymerCheckboxValueAccessor implements ControlValueAccessor {
  private delegateOnChange = (_: any) => {};
  
  constructor(private _renderer: Renderer, private _elementRef: ElementRef) {}
  
  public writeValue(value: any): void {
    if (value)
      (<HTMLElement>this._elementRef.nativeElement).setAttribute("checked", "true");
    else
      (<HTMLElement>this._elementRef.nativeElement).removeAttribute("checked");
  }

  public registerOnChange(fn: (_: any) => void): void { 
    this.delegateOnChange = fn; 
  }
  
  public registerOnTouched(fn: () => void): void {
    // noop, cannot be touched. 
  }
  
  private onChange(newValue: any): void {
    this.delegateOnChange(newValue);
  }
  
}


/**
 * A ControlValueAccessor that binds to paper-input and paper-textarea. In addition to writing the values to the element,
 * this class also evaluates errors reported on the NgControl and displays them on the paper-input/paper-textarea.
 */
@Directive({
  selector: "[dropDownModel][ngModel]",
  host: {
    "(iron-select)": "onChange($event.target.selected)"
  },
  bindings: [ POLYMER_DROPDOWN_VALUE_ACCESSOR ]
})
export class PolymerDropdownValueAccessor implements ControlValueAccessor {
  private delegateOnChange = (_: any) => {};
  
  constructor(private _renderer: Renderer, private _elementRef: ElementRef) {}
  
  public writeValue(value: any): void {
    (<HTMLElement>this._elementRef.nativeElement).setAttribute("selected", value);
  }

  public registerOnChange(fn: (_: any) => void): void { 
    this.delegateOnChange = fn; 
  }
  
  public registerOnTouched(fn: () => void): void {
    // noop, cannot be touched. 
  }
  
  private onChange(newValue: any): void {
    this.delegateOnChange(newValue);
  }
  
}
