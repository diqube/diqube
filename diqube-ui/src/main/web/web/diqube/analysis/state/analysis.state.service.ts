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

import {Injectable} from "angular2/core";

@Injectable()
export class AnalysisStateService {
  private queriesToOpenInEditMode: Array<string> = [];
  private slicesToOpenInEditMode: Array<string> = [];
  
  /**
   * Mark the given query to be "opened" the next time when it is rendered in the UI. This is meaningful for newly
   * created queries for example, as they should be editable for the user right after clicking an "add" button or
   * something.
   */
  public markToOpenQueryInEditModeNextTime(queryId: string): void {
    this.queriesToOpenInEditMode.push(queryId);
  }

  /**
   * Check if the given query should be opened and remove the state.
   */
  public pollOpenQueryInEditModeNextTime(queryId: string): boolean {
    var idx: number = this.queriesToOpenInEditMode.indexOf(queryId);
    if (idx === -1)
      return false;
    
    this.queriesToOpenInEditMode.splice(idx, 1);
    return true;
  }
  
  /**
   * Mark the given slice to be "opened" the next time when it is rendered in the UI. This is meaningful for newly
   * created slices for example, as they should be editable for the user right after clicking an "add" button or
   * something.
   */
  public markToOpenSliceInEditModeNextTime(sliceId: string): void {
    this.slicesToOpenInEditMode.push(sliceId);
  }
  
  /**
   * Check if the given slice should be opened and remove the state.
   */
  public pollOpenSliceInEditModeNextTime(sliceId: string): boolean {
    var idx: number = this.slicesToOpenInEditMode.indexOf(sliceId);
    if (idx === -1)
      return false;
    
    this.slicesToOpenInEditMode.splice(idx, 1);
    return true;
  }
}