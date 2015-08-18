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
package org.diqube.data.lng.array;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.DataSerializationDelegationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArray;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayBitEfficient;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayRLE;
import org.diqube.data.serialize.thrift.v1.SLongCompressedArrayReference;
import org.diqube.util.Pair;

/**
 * A {@link DataSerializationDelegationManager} for {@link CompressedLongArray}.
 *
 * @author Bastian Gloeckle
 */
public class CompressedLongArrayDeserializationDelegationManager
    implements DataSerializationDelegationManager<SLongCompressedArray> {

  @Override
  public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(
      SLongCompressedArray serialized) throws DeserializationException {
    if (serialized.isSetBitEfficient())
      return new Pair<>(BitEfficientLongArray.class, serialized.getBitEfficient());
    if (serialized.isSetRef())
      return new Pair<>(ReferenceBasedLongArray.class, serialized.getRef());
    if (serialized.isSetRle())
      return new Pair<>(RunLengthLongArray.class, serialized.getRle());
    throw new DeserializationException("Unknown compressed long array type");
  }

  @Override
  public <O extends TBase<?, ?>> SLongCompressedArray serializeWrapObject(O obj) throws SerializationException {
    SLongCompressedArray res = new SLongCompressedArray();
    if (obj instanceof SLongCompressedArrayBitEfficient)
      res.setBitEfficient((SLongCompressedArrayBitEfficient) obj);
    else if (obj instanceof SLongCompressedArrayReference)
      res.setRef((SLongCompressedArrayReference) obj);
    else if (obj instanceof SLongCompressedArrayRLE)
      res.setRle((SLongCompressedArrayRLE) obj);
    else
      throw new SerializationException("Cannot wrap " + obj);
    return res;
  }

}
