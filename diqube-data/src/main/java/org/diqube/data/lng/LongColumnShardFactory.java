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
package org.diqube.data.lng;

import java.util.NavigableMap;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.colshard.ColumnPage;
import org.diqube.data.colshard.ColumnShardFactory;
import org.diqube.data.lng.dict.LongDictionary;

/**
 * A Factory for {@link LongColumnShard}s.
 * 
 * Users might want to use the {@link ColumnShardFactory}. This class is only here, to access the package-private
 * constructor.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class LongColumnShardFactory {
  public LongStandardColumnShard createStandardLongColumnShard(String colName, NavigableMap<Long, ColumnPage> pages,
      LongDictionary columnDictionary) {
    return new LongStandardColumnShard(colName, pages, columnDictionary);
  }

  public LongConstantColumnShard createConstantLongColumnShard(String colName, Long value, long firstRowId) {
    return new LongConstantColumnShard(colName, value, firstRowId);
  }
}
