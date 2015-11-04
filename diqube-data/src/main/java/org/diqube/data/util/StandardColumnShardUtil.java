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
package org.diqube.data.util;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.diqube.data.column.AdjustableColumnPage;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.StandardColumnShard;

/**
 * Util methods for {@link StandardColumnShard}.
 *
 * @author Bastian Gloeckle
 */
public class StandardColumnShardUtil {

  /**
   * Walks along all given input pages and adjusts their firstRowId by the given delta.
   * 
   * @param rowIdDelta
   *          The delta to apply to the firstRowIds of the tables
   * @return A new pages map.
   * @throws UnsupportedOperationException
   *           If not all pages are {@link AdjustableColumnPage}s.
   */
  public NavigableMap<Long, ColumnPage> adjustFirstRowIdOnPages(NavigableMap<Long, ColumnPage> inputPages,
      long rowIdDelta, String colName) throws UnsupportedOperationException {
    if (inputPages.values().stream().anyMatch(page -> !(page instanceof AdjustableColumnPage)))
      throw new UnsupportedOperationException("Cannot adjust rowIDs, because not all ColumnPages are adjustable.");

    NavigableMap<Long, ColumnPage> newPages = new TreeMap<>();
    for (Entry<Long, ColumnPage> pageEntry : inputPages.entrySet()) {
      ColumnPage page = pageEntry.getValue();
      ((AdjustableColumnPage) page).setFirstRowId(page.getFirstRowId() + rowIdDelta);
      ((AdjustableColumnPage) page).setName(colName + "#" + page.getFirstRowId());
      newPages.put(page.getFirstRowId(), page);
    }

    return newPages;
  }
}
