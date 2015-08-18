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

/**
 * Implements de-/serialization of classes in diqube-data into thrift objects from diqube-data-serialize.
 *
 * Classes implementing this interface should also carry the {@link DataSerializable} annotation and need to have a
 * public non-arg constructor.
 *
 * @param <T>
 *          A Thrift class from diqube-data-serialize which mirrors the serialized values of this class.
 * 
 * @author Bastian Gloeckle
 */
public interface DataSerialization<T extends TBase<?, ?>> {
  /**
   * Serialize the current object into the given target object.
   * 
   * @throws SerializationException
   *           if anything went wrong.
   */
  public void serialize(DataSerializationHelper helper, T target) throws SerializationException;

  /**
   * Deserialize from the given source object into the current object.
   * 
   * @throws DeserializationException
   *           if anything went wrong.
   */
  public void deserialize(DataSerializationHelper helper, T source) throws DeserializationException;

  /**
   * A {@link DataSerializationHelper} has more overview over the de-/serialization process and can be used to
   * de-/serialize other objects, e.g. child objects.
   */
  public interface DataSerializationHelper {
    public <M extends TBase<?, ?>, I extends DataSerialization<M>, O extends TBase<?, ?>> O serializeChild(
        Class<? extends O> targetClass, I obj) throws SerializationException;

    public <I extends TBase<?, ?>, M extends TBase<?, ?>, O extends DataSerialization<M>> O deserializeChild(
        Class<? extends O> targetClass, I obj) throws DeserializationException;
  }
}
