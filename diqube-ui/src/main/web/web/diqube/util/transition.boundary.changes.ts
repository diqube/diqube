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

import {AfterViewChecked} from "angular2/core";

/**
 * Helper class to get a nice CSS transition on boundary changes of a HTMLElement.
 * 
 * This is needed, as CSS transitions do not simply work when a "width: auto" or "height: auto" gets adjusted (e.g. when
 * changing the DOM), as this is poorly supported by browsers. This class implements a nice transition of an
 * element.
 * 
 * How to use this:
 * 
 * * Construct an object with a given dimension ("width" or "height") and an element that should be transitioned.
 * * In the component hosting the element, implement AfterViewChecked and always call this objects "ngAfterViewChecked()"
 *   method.
 * * When executing code that will change the size of the given element, then execute this call surrounded by a call to
 *   "transitionBoundaryChange()".
 * 
 * Example:
 * 
 * Component template: "<div style='width: {{ width }}px'>Hello world</div>
 * Component source:
 *    private width: number = 100;
 * 
 *    public onDoSomething() {
 *      transitionBoundaryChanges.transitionBoundaryChange(() => { this.width = 200; return Promise.resolve(null); });
 * 
 *      // do NOT do the following:
 *      this.width = 200;
 *    }
 */
export class TransitionBoundaryChanges implements AfterViewChecked {
  public static TRANSITION_TIMING_FUNCTION_EASE: string = "ease";
  public static TRANSITION_TIMING_FUNCTION_EASE_IN: string = "ease-in";
  public static TRANSITION_TIMING_FUNCTION_EASE_OUT: string = "ease-out";

  
  /* time the transition takes. Keep in sync with styles.html for transitions that are only defined in CSS. */
  public static DEFAULT_TRANSITION_TIME_MS: number = 300;
  public static DEFAULT_TRANSITION_TIMING_FUNCTION: string = TransitionBoundaryChanges.TRANSITION_TIMING_FUNCTION_EASE;
  
  private doOnAfterViewChecked: ()=>void;

  private currentPromise: Promise<void>;
  
  constructor (private element: HTMLElement, private dimension: string) {}
  
  /**
   * Make a smooth CSS transition of the change that will increase/decrease the elements size.
   * 
   * @param fn: The function that changes the component field that, when rendered by angular, will change the elements
   *        size. Before executing this function, transitionBoundaryChange will fetch the width/height of the
   *        element to start the transition at. Then it will execute the function and wait for the returned promise 
   *        to complete, then it starts the transition of width/height to the value that is computed by the browser
   *        after applying the changes of the function.
   * @param transitionTimeMs: optional. Milliseconds the transition should take.
   * @param transitionTimingFunction: optional. A value for CSS "transition-timing-function".
   * @param afterTransitionStart: optional. Function will be called after the target width has been
   *        calculated and the transition has started, but not yet ended.
   * @returns a Promise that completes when the height/width transition finishes
   */
  public transitionBoundaryChange(fn: () => Promise<void>, transitionTimeMs?: number, transitionTimingFunction?: string, 
                                  afterTransitionStart?: () => void): Promise<void> {
    if (this.currentPromise)
      // transition in progress. Let the current transition complete and then execute the new one.
      return this.currentPromise.then(() => { 
        this.transitionBoundaryChange(fn, transitionTimeMs, transitionTimingFunction, afterTransitionStart); 
      });
    
    if (transitionTimeMs === undefined)
      transitionTimeMs = TransitionBoundaryChanges.DEFAULT_TRANSITION_TIME_MS;
    if (transitionTimingFunction === undefined)
      transitionTimingFunction = TransitionBoundaryChanges.DEFAULT_TRANSITION_TIMING_FUNCTION;
    
    this.currentPromise = new Promise((resolve: (a: void)=>void, reject: (msg: string) => void) => {
      var previousValue: string = (<any>getComputedStyle(this.element))[this.dimension];
      
      fn().then(() => {
         
        // work on the DOM after all changes have been integrated into it (Polymer has run there, too): run in ngAfterViewChecked().
        
        // Do not execute in next tick (setTimeout), otherwise we'd see a very quick "flash" of the already expanded
        // element before it is shrinked again and then transitioned open.
        this.doOnAfterViewChecked = () => {
          (<any>this.element.style)[this.dimension] = "auto";
          this.repaint();
          var newValue: string = (<any>getComputedStyle(this.element))[this.dimension];
          if (newValue === previousValue) {
            this.currentPromise = undefined;
            resolve(null);
            return;
          }
          
          (<any>this.element.style)[this.dimension] = previousValue;
          this.repaint();
          this.element.style.transition = this.dimension + " " + transitionTimeMs + "ms " + transitionTimingFunction;
          (<any>this.element.style)[this.dimension] = newValue;
          
          if (afterTransitionStart) {
            afterTransitionStart();
          }
          
          var transitionedListener: (event: TransitionEvent) => void;
          transitionedListener = (event: TransitionEvent) => {
            if (event.propertyName === this.dimension) {
              this.element.style.transition = "";
              (<any>this.element.style)[this.dimension] = "auto";
              this.currentPromise = undefined;
              
              this.element.removeEventListener("transitionend", transitionedListener);
              
              resolve(null);
            }
          };
          
          this.element.addEventListener("transitionend", transitionedListener);
        };
      });
    });
    
    return this.currentPromise;
  }
  
  /**
   * Call this method in the components ngAfterViewChecked method.
   */
  public ngAfterViewChecked(): any {
    if (this.doOnAfterViewChecked) {
      this.doOnAfterViewChecked();
      this.doOnAfterViewChecked = undefined;
    }
  };
  
  /**
   * @returns true if a transition has been requested by a call to transitionBoundaryChange which has not fully transitioned yet.
   */
  public isTransitioning(): boolean {
    return this.currentPromise !== undefined;
  }
  
  private repaint(): void {
    // forces repaint, see e.g. http://stackoverflow.com/questions/3485365/how-can-i-force-webkit-to-redraw-repaint-to-propagate-style-changes/3485654#3485654
    // Does apply all changes to the objects style e.g. to get applied and widths etc get recomputed.
    this.element.offsetWidth = this.element.offsetWidth; // assign to itself so the call does not get optimized away. 
  }
}