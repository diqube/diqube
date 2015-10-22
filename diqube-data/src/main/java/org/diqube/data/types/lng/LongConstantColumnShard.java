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
package org.diqube.data.types.lng;

import org.diqube.data.column.AbstractConstantColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.ConstantColumnShard;
import org.diqube.data.types.lng.dict.ConstantLongDictionary;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * A {@link ConstantColumnShard} of a column containing long values.
 *
 * @author Bastian Gloeckle
 */
public class LongConstantColumnShard extends AbstractConstantColumnShard implements LongColumnShard {

  protected LongConstantColumnShard(String name, Object value, long firstRowId) {
    super(ColumnType.LONG, name, value, firstRowId);
  }

  @Override
  protected LongDictionary<?> createColumnShardDictionary(Object value) {
    return new ConstantLongDictionary((Long) value, 0L);
  }

  @Override
  public LongDictionary<?> getColumnShardDictionary() {
    return (LongDictionary<?>) super.getColumnShardDictionary();
  }

}
