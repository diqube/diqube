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

import {UiQuery, TableJsonResult} from "../remote/remote";

/**
 * A normal TableJsonResult which is enhanced by additional fields needed for keeping state in the UI.
 */
export type EnhancedTableJsonResult = TableJsonResult & {
  /** An exception that occurred while calculating the result. If this field is set, no other of the EnhancedTableJsonResult should be used. */ 
  exception: string 
};

export interface UiQueryWithResults extends UiQuery {
  /** 
   * The current results available for this query. Leading "$" so these values do not get transferred to the server by 
   * mistake (RemoteService filters out all props starting with $).
   * 
   * A value of "undefined" means that this UiQuery does not have any results.
   * A value of "null" means that the query was sent for execution, but no results did yet arrive.
   * If there are results, they will be available in this field, obviously. 
   */
  $results: EnhancedTableJsonResult;
};

/**
 * Returns true if the given UiQuery is actually a UiQueryWithResults.
 */
export function isUiQueryWithResults(query: UiQuery): boolean {
  return (<UiQueryWithResults> query).$results !== undefined;
}

/**
 * Enhance the given UiQuery to become a UiQueryWithResults with the given TableJsonResult object.
 */
export function enhanceUiQueryWithResults(query: UiQuery, $results: TableJsonResult): UiQueryWithResults {
  (<UiQueryWithResults> query).$results = <EnhancedTableJsonResult>$results; // note that the properties added to TableJsonResult will be undefined!
  return <UiQueryWithResults> query;
} 