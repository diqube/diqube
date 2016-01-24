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


/**
 * Arbitrary data that is attached to a drag. See implementing classes.
 */
export interface DragDropData {
}

/**
 * Data that is being dragged, available from DragDropService.
 * 
 * The type field denotes the data that is being dragged, see the TYPE field of the concrete data classes.
 */
export class DragDropElement {
  constructor(public type: string, public data: DragDropData) {}
}

/**
 * Data of a "restriction" that is being dragged. 
 * 
 * A restriction is the pair of a field name and a value. When it is dropped, the restriction is usually added to 
 * something (e.g. to a slice).
 */
export class DragDropRestrictionData implements DragDropData {
  public static TYPE: string = "TYPE_RESTRICTION";
  
  constructor(public field: string, public value: string) {}
}
