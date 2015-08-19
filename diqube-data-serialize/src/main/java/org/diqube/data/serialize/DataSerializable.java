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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.thrift.TBase;
import org.diqube.util.Pair;

/**
 * Marks a class/interface as being part of diqube-data de-/serialization. The annotation needs to carry the
 * {@link DataSerializable#thriftClass()} property which maps a class generated by Thrift in diqube-data-serialize to
 * the current class - both those classes are then marked as carrying the same data logically: The thrift class contains
 * the "serialized" data, the class carrying this annotation carries the deserialized data.
 * 
 * <p>
 * If an interface carries this annotation, it cannot be deserialized directly. It therefore needs to supply a
 * {@link DataSerializable#deserializationDelegationManager()} in order to delegate the deserialization to another
 * class, based on the serialized object.
 * 
 * <p>
 * A class/interface carrying this annotation needs to implement {@link DataSerialization} accordingly (generic
 * parameter is the same as {@link #thriftClass()}).
 *
 * @author Bastian Gloeckle
 */
@Target(value = ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DataSerializable {
  /** The class that is used to serialize the data of the annotated class */
  Class<? extends TBase<?, ?>>thriftClass();

  /**
   * In case the annotation is on an interface (which cannot be deserialized right away), this
   * {@link DataSerializationDelegationManager} helps finding the right class to deserialize into.
   */
  Class<? extends DataSerializationDelegationManager<?>>deserializationDelegationManager() default DataSerializable.NONE.class;

  public static class NONE implements DataSerializationDelegationManager<TBase<?, ?>> {
    @Override
    public Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> getDeserializationDelegate(TBase<?, ?> serialized) {
      return null;
    }

    @Override
    public <O extends TBase<?, ?>> TBase<?, ?> serializeWrapObject(O obj) {
      return null;
    }
  };
}