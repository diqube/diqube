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
package org.diqube.data.column;

import org.apache.thrift.TBase;
import org.diqube.data.dbl.DoubleStandardColumnShard;
import org.diqube.data.lng.LongStandardColumnShard;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationDelegationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SColumnShard;
import org.diqube.data.str.StringStandardColumnShard;
import org.diqube.util.Pair;

/**
 * A {@link DataSerializationDelegationManager} for column shards.
 *
 * @author Bastian Gloeckle
 */
public class StandardColumnShardDeserializationDelegationManager
    implements DataSerializationDelegationManager<SColumnShard> {

  @Override
  public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(SColumnShard serialized)
      throws DeserializationException {
    Class<? extends DataSerialization<?>> resClass;
    switch (serialized.getType()) {
    case STRING:
      resClass = StringStandardColumnShard.class;
      break;
    case LONG:
      resClass = LongStandardColumnShard.class;
      break;
    case DOUBLE:
      resClass = DoubleStandardColumnShard.class;
      break;
    default:
      throw new DeserializationException("Cannot deserialize column shard: unknown type.");
    }
    return new Pair<>(resClass, serialized);
  }

  @Override
  public <O extends TBase<?, ?>> SColumnShard serializeWrapObject(O obj) throws SerializationException {
    if (!(obj instanceof SColumnShard))
      throw new SerializationException("Cannot wrap " + obj);
    return (SColumnShard) obj;
  }

}
