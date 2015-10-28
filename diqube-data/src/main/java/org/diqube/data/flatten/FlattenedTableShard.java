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

import java.util.Collection;

import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DataSerializableIgnore;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.STableShard;
import org.diqube.data.table.DefaultTableShard;

/**
 * A table shard of a flattened table.
 *
 * @author Bastian Gloeckle
 */
@DataSerializableIgnore
public class FlattenedTableShard extends DefaultTableShard {

  /* package */ FlattenedTableShard(String tableName, Collection<StandardColumnShard> columns) {
    super(tableName, columns);
  }

  @Override
  public void serialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper, STableShard target)
      throws SerializationException {
    throw new SerializationException("Cannot serialize a flattened table shard.");
  }

  @Override
  public void deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationHelper helper,
      STableShard source) throws DeserializationException {
    throw new DeserializationException("Cannot deserialize a flattened table shard.");
  }

}
