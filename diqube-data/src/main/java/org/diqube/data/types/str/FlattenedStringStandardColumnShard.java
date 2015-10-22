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

import org.diqube.data.column.ColumnPage;
import org.diqube.data.flatten.AbstractFlattenedStandardColumnShard;
import org.diqube.data.types.str.dict.StringDictionary;

/**
 *
 * @author Bastian Gloeckle
 */
public class FlattenedStringStandardColumnShard extends AbstractFlattenedStandardColumnShard
    implements StringColumnShard {

  /* package */ FlattenedStringStandardColumnShard(String name, StringDictionary<?> columnShardDict, long firstRowId,
      List<ColumnPage> pages) {
    super(name, columnShardDict, firstRowId, pages);
  }

  @Override
  public StringDictionary<?> getColumnShardDictionary() {
    return (StringDictionary<?>) super.getColumnShardDictionary();
  }
}
