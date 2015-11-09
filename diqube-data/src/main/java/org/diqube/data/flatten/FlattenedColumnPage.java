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
package org.diqube.data.flatten;

import org.diqube.data.column.ColumnPage;
import org.diqube.data.column.DefaultColumnPage;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * A {@link ColumnPage} of a flattened table shard, which typically (indirectly) references the data of the original
 * table.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class FlattenedColumnPage extends DefaultColumnPage {

  /* package */ FlattenedColumnPage(LongDictionary<?> columnPageDict, CompressedLongArray<?> values, long firstRowId,
      String name) {
    super(columnPageDict, values, firstRowId, name);
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, SColumnPage target)
      throws SerializationException {
    throw new SerializationException("Cannot serialize flattened ColPage.");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      SColumnPage source) throws DeserializationException {
    throw new DeserializationException("Cannot deserialize flattened ColPage.");
  }

  @Override
  public long calculateApproximateSizeInBytes() {
    return super.calculateApproximateSizeInBytes();
  }

}
