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
package org.diqube.data.colshard;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.lng.array.CompressedLongArray;
import org.diqube.data.lng.dict.LongDictionary;

/**
 * Factory for {@link ColumnPage}s.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ColumnPageFactory {

  /**
   * Create a new {@link ColumnPage}.
   * 
   * @param columnPageDict
   *          The Column Page Dictionary as described in the JavaDoc of {@link ColumnPage}.
   * @param values
   *          The values of this column page, available in Column Page Value IDs that are available in ColumnPageDict.
   * @param firstRowId
   *          The ID of the first row whose value is represented by the first value in 'values'.
   * @param name
   *          The name of the colPage. This should denote the column it belongs to and which of the columnPages of that
   *          column it is.
   * @return The new {@link ColumnPage}.
   */
  public ColumnPage createColumnPage(LongDictionary<?> columnPageDict, CompressedLongArray<?> values, long firstRowId,
      String name) {
    return new ColumnPage(columnPageDict, values, firstRowId, name);
  }
}
