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

import org.diqube.data.column.AbstractConstantColumnShard;
import org.diqube.data.column.ColumnType;
import org.diqube.data.column.ConstantColumnShard;
import org.diqube.data.types.dbl.dict.ConstantDoubleDictionary;
import org.diqube.data.types.dbl.dict.DoubleDictionary;

/**
 * A {@link ConstantColumnShard} of a column containing double values.
 *
 * @author Bastian Gloeckle
 */
public class DoubleConstantColumnShard extends AbstractConstantColumnShard implements DoubleColumnShard {

  protected DoubleConstantColumnShard(String name, Object value, long firstRowId) {
    super(ColumnType.DOUBLE, name, value, firstRowId);
  }

  @Override
  protected DoubleDictionary<?> createColumnShardDictionary(Object value) {
    return new ConstantDoubleDictionary((Double) value);
  }

  @Override
  public DoubleDictionary<?> getColumnShardDictionary() {
    return (DoubleDictionary<?>) super.getColumnShardDictionary();
  }

}
