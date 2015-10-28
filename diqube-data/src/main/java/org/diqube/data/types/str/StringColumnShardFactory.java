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
package org.diqube.data.types.str;

import java.util.List;
import java.util.NavigableMap;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnShardFactory;
import org.diqube.data.types.str.dict.StringDictionary;

/**
 * A Factory for {@link StringColumnShard}s.
 * 
 * Users might want to use the {@link ColumnShardFactory}. This class is only here, to access the package-private
 * constructor.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class StringColumnShardFactory {
  public StringStandardColumnShard createStandardStringColumnShard(String colName, NavigableMap<Long, ColumnPage> pages,
      StringDictionary<?> columnDictionary) {
    return new DefaultStringStandardColumnShard(colName, pages, columnDictionary);
  }

  public StringConstantColumnShard createConstantStringColumnShard(String colName, String value, long firstRowId) {
    return new StringConstantColumnShard(colName, value, firstRowId);
  }

  public FlattenedStringStandardColumnShard createFlattenedStringStandardColumnShard(String name,
      StringDictionary<?> columnShardDict, long firstRowId, List<ColumnPage> pages) {
    return new FlattenedStringStandardColumnShard(name, columnShardDict, firstRowId, pages);
  }

}
