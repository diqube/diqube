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
package org.diqube.data.serialize;

import org.apache.thrift.TBase;
import org.diqube.util.Pair;

/**
 * Handles cases where serialized/non-serialized classes do not map 1:1, but need to be re-mapped.
 *
 * @author Bastian Gloeckle
 */
public interface DataSerializationDelegationManager<T extends TBase<?, ?>> {

  /**
   * @return A pair of a class and a serialized object. The class is that class on which
   *         {@link DataSerialization#deserialize(org.diqube.data.serialize.DataSerialization.DataSerializationManager, Object)}
   *         should be called for the given serialized object. The Object is a serialized object which should be given
   *         to a new instance of that class from which it should deserialize.
   * @throws DeserializationException
   *           if anything went wrong.
   */
  public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(T serialized)
      throws DeserializationException;

  /**
   * Wraps a serialization object into another serialization object.
   */
  public <O extends TBase<?, ?>> T serializeWrapObject(O obj) throws SerializationException;
}
