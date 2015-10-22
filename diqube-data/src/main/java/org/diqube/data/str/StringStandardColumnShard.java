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
package org.diqube.data.str;

import java.util.NavigableMap;

import org.diqube.data.column.AbstractStandardColumnShard;
import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnType;
import org.diqube.data.str.dict.StringDictionary;

/**
 * A {@link StringColumnShard} of a column containing string values.
 *
 * @author Bastian Gloeckle
 */
public class StringStandardColumnShard extends AbstractStandardColumnShard implements StringColumnShard {

  /** for deserialization */
  public StringStandardColumnShard() {
    super();
  }

  StringStandardColumnShard(String name, NavigableMap<Long, ColumnPage> pages, StringDictionary<?> columnDictionary) {
    super(ColumnType.STRING, name, pages, columnDictionary);
  }

  @Override
  public StringDictionary<?> getColumnShardDictionary() {
    return (StringDictionary<?>) super.getColumnShardDictionary();
  }
}
