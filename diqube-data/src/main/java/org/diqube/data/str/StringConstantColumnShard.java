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

import org.diqube.data.ColumnType;
import org.diqube.data.colshard.AbstractConstantColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.str.dict.ConstantStringDictionary;
import org.diqube.data.str.dict.StringDictionary;

/**
 * A {@link ConstantColumnShard} of a column containing String values.
 *
 * @author Bastian Gloeckle
 */
public class StringConstantColumnShard extends AbstractConstantColumnShard implements StringColumnShard {

  protected StringConstantColumnShard(String name, Object value, long firstRowId) {
    super(ColumnType.STRING, name, value, firstRowId);
  }

  @Override
  protected StringDictionary<?> createColumnShardDictionary(Object value) {
    return new ConstantStringDictionary((String) value, 0L);
  }

  @Override
  public StringDictionary<?> getColumnShardDictionary() {
    return (StringDictionary<?>) super.getColumnShardDictionary();
  }

}
