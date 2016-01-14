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

import {Pipe, PipeTransform} from "angular2/core";

export class IterationItem {
  key: any;
  value: any;
}

/**
 * Pipe that takes a object as input (= a map) and returns its fields in sorted key order, so it can be used for ngFor.
 */
@Pipe({name: "iterateMapSorted"})
export class IterateMapSortedPipe implements PipeTransform {
  public transform(value: any, args: string[]) : Array<IterationItem> {
    var keys: Array<any> = [];
    for (var key in value) {
      if (value.hasOwnProperty(key)) {
        keys.push(key);
      }
    }
    
    keys.sort();
    
    var res: Array<IterationItem> = [];
    for (var idx in keys) {
      var key: any = keys[idx];
      res.push({ 
        key: key,
        value: value[key]
      }); 
    }
    
    return res;
  }
}

/**
 * Pipe that cuts off all input string as a configurable amount of characters
 */
@Pipe({name: "limitTo"})
export class LimitToPipe implements PipeTransform {
  public transform(value: string, args: string[]) : string {
    var targetLen: number = parseInt(args[0]);
    if (value.length > targetLen) 
      value = value.substr(0, targetLen);
    
    return value;
  }
}