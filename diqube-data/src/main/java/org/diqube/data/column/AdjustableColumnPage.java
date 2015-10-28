/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.data.column;

/**
 * A {@link ColumnPage} where things are adjustable.
 *
 * @author Bastian Gloeckle
 */
public interface AdjustableColumnPage extends ColumnPage {

  /**
   * Sets the first row ID. Only to be used when {@link AdjustableStandardColumnShard#adjustToFirstRowId(long)} is
   * called.
   */
  public void setFirstRowId(long firstRowId);

  /**
   * Sets the name of the colPage. Only to be used before the colPage is live.
   */
  public void setName(String name);
}
