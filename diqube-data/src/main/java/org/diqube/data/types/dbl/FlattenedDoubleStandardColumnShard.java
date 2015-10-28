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
package org.diqube.data.types.dbl;

import java.util.List;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.ColumnType;
import org.diqube.data.flatten.AbstractFlattenedStandardColumnShard;
import org.diqube.data.types.dbl.dict.DoubleDictionary;

/**
 *
 * @author Bastian Gloeckle
 */
public class FlattenedDoubleStandardColumnShard extends AbstractFlattenedStandardColumnShard
    implements DoubleStandardColumnShard {

  /* package */ FlattenedDoubleStandardColumnShard(String name, DoubleDictionary<?> columnShardDict, long firstRowId,
      List<ColumnPage> pages) {
    super(name, ColumnType.DOUBLE, columnShardDict, firstRowId, pages);
  }

  @Override
  public DoubleDictionary<?> getColumnShardDictionary() {
    return (DoubleDictionary<?>) super.getColumnShardDictionary();
  }
}
