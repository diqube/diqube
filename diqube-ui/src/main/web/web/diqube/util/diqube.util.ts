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

// set in index.html
declare var globalContextPath: string;

// uuid library
declare var uuid: any;

// chart.js library
declare var Chart: any;

// mdl
declare var componentHandler: any;

export class DiqubeUtil {
  /**
   * Deep-copy an arbitrary object. 
   */
  static copy<T>(input: T): T {
    if (typeof input === "string" || typeof input === "number" || typeof input === "boolean" || input === null)
      return input;
    
    return JSON.parse(JSON.stringify(input));
  }
  
  /**
   * Just like #copy, but copies all values (recursievly) into an object that is already available instead of creating a
   * new object.
   * 
   * Does NOT copy any field starting with "$", NO functions and NO symbols.
   * 
   * It will NOT reset a field if that has the correct value already!
   * 
   * Does NOT remove any properties from output, except for array-indices that are not present in input. 
   * 
   * @returns the output object that was passed as parameter, unless input is native type, then output is not adjusted,
   *          but input is returned.
   */
  static copyTo<T>(input: T, output: T): T {
    if (typeof input === "string" || typeof input === "number" || typeof input === "boolean" || input === null)
      return input;
    
    for (var k in input) {
      if (k.indexOf("$") != 0 && input.hasOwnProperty(k)) {
        var value: any = (<any>input)[k];
        
        if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
          if (value !== (<any>output)[k])
            (<any>output)[k] = value;
        } else if (typeof value === "object") {
          if (Array.isArray(value)) {
            if (!output.hasOwnProperty(k) || (<any>output)[k] === null || !(typeof (<any>output)[k] === "object") || !Array.isArray((<any>output)[k]))
              (<any>output)[k] = [];
            
            for (var idx in value) {
              if ((<any>output)[k].length <= idx)
                (<any>output)[k].push(DiqubeUtil.copy(value[idx]));
              else
                // assign in case value[idx] is native type
                (<any>output)[k][idx] = DiqubeUtil.copyTo(value[idx], (<any>output)[k][idx]); 
            }
            
            if ((<any>output)[k].length > value.length)
              // remove objects that are too much in output.
              (<any>output)[k].splice(value.length, (<any>output)[k].length - value.length);
             
          } else {
            if (!output.hasOwnProperty(k) || (<any>output)[k] === null || !(typeof (<any>output)[k] === "object") || Array.isArray((<any>output)[k]))
              (<any>output)[k] = {};
            DiqubeUtil.copyTo((<any>input)[k], (<any>output)[k]);
          }
        }
      }
    }
    return output;
  }
  
  /**
   * The context path under which the webapp is deployed. 
   * 
   * Examples: 
   *  URL = http://localhost:8080/diqube-ui -> contextPath = "/diqube-ui" 
   *  URL = http://localhost:8080/diqube-ui/about -> contextPath = "/diqube-ui" 
   */
  static globalContextPath(): string {
    return globalContextPath;
  }
  
  /**
   * Recursively compares "a" and "b" and returns true if both object trees are equal.
   * 
   * Ignores all properties starting with "$". Additionally, "function" and "symbol" are ignored.
   */
  static equals(a: any, b: any): boolean {
    if (typeof a !== typeof b)
      return false;
    
    if (a === b)
      return true;
    
    if (typeof a === "function" || typeof a === "symbol")
      // ignore.
      return true;
    
    if (typeof a === "object") {
      if ((a == null && b != null) || (a != null && b == null))
        // "null" is "object", check if only one of a/b is null.
        return false;

      if (Array.isArray(a)) {
        if (!Array.isArray(b) || a.length !== b.length)
          return false;
        
        var i: number = 0;
        while (i < a.length) {
          if (!DiqubeUtil.equals(a[i], b[i]))
            return false;
          i += 1;
        }
        return true;
      } else {
        // traverse all properties of a, check if b has the same property and validate if they are equal recursively.
        var allAProps : Array<string> = [];
        for (var prop in a) {
          if (prop.startsWith("$"))
            continue;
          if (a.hasOwnProperty(prop)) {
            allAProps.push(prop);
            if (!b.hasOwnProperty(prop) || !DiqubeUtil.equals(a[prop], b[prop]))
              // b does not contain property or the values differ.
              return false;
          }
        }
        allAProps.sort();
        
        // quickly retrieve all props from b. Do not again recursively check everything, as if a and b are inequal, b
        // could only have more properties than a, since we checked all of a's properties recursively already.
        var allBProps : Array<string> = [];
        for (var prop in b) {
          if (prop.startsWith("$"))
            continue;
          if (b.hasOwnProperty(prop)) {
            allBProps.push(prop);
          }
        }
        allBProps.sort();

        // compare props one-by-one: The arrays should be totally equal, as they're sorted.
        while (allAProps.length > 0 && allBProps.length > 0) {
          var aProp: string = allAProps.shift();
          var bProp: string = allBProps.shift();
          if (aProp !== bProp)
            return false;
        }
        
        if (allAProps.length > 0 || allBProps.length > 0)
          // one has more props than the other
          return false;
        
        return true;
      }
    }  
    
    // "unknown" objects?!
    return false;
  }
  
  /**
   * Generates and returns a new UUID.
   */
  public static newUuid(): string {
    return uuid.v4();
  }
  
  /**
   * Create new "Chart" object from chart.js.
   */
  public static newChart(htmlElement: HTMLElement, configuration: any): any {
    return new Chart(htmlElement, configuration);
  }
  
  /**
   * The "componentHandler" of Material Design Lite.
   */
  public static mdlComponentHandler(): any {
    return componentHandler;
  }
}
