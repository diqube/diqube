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
package org.diqube.data;

import org.apache.thrift.TBase;
import org.diqube.data.serialize.DataSerializable;
import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.thrift.v1.SDictionary;

/**
 * A {@link Dictionary} that is {@link DataSerializable}.
 *
 * @param <T>
 *          type of data the dict contains
 * @param <S>
 *          serialized class this dict serializes to/from.
 *
 * @author Bastian Gloeckle
 */
@DataSerializable(thriftClass = SDictionary.class,
    deserializationDelegationManager = DictionaryDeserializationDelegationManager.class)
public interface SerializableDictionary<T, S extends TBase<?, ?>> extends Dictionary<T>, DataSerialization<S> {

}
