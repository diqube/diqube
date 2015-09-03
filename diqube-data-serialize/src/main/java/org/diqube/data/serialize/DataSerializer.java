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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.data.serialize.DataSerialization.DataSerializationHelper;

/**
 * Serializes any class implementing {@link DataSerialization} to thrift objects and then to a stream.
 * 
 * Does not put any header/metainformation/etc into the stream, but only serializes a given object.
 * 
 * An instance of this class can be re-used.
 * 
 * @author Bastian Gloeckle
 */
public class DataSerializer {
  /** Version of thrift objects created by this serializer/capable of reading in the deserializer. */
  public static final int DATA_VERSION = 1;

  private Map<Class<? extends DataSerialization<?>>, Class<? extends TBase<?, ?>>> thriftClasses;

  private Function<ObjectDoneConsumer, DataSerializationHelper> dataSerializationHelperFactory =
      (objectDoneConsumer) -> {
        return new DataSerializationHelper() {

          @SuppressWarnings("unchecked")
          @Override
          public <M extends TBase<?, ?>, I extends DataSerialization<M>, O extends TBase<?, ?>> O serializeChild(
              Class<? extends O> targetClass, I obj) throws SerializationException {
            if (obj == null)
              return null;

            // serialize object into that thrift object which the class itself supports
            Class<?> thriftClass = thriftClasses.get(obj.getClass());
            M target;
            try {
              target = (M) thriftClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
              throw new SerializationException("Could not instantiate " + thriftClass);
            }
            obj.serialize(this, target);

            objectDoneConsumer.accept(obj);

            // if the target object is already of the requested type, we're done!
            if (targetClass.isInstance(target))
              return (O) target;

            // target object is of different type than requested. Try to wrap it using a delegationManager.
            DataSerializationDelegationManager<O> delegationManager =
                (DataSerializationDelegationManager<O>) delegationManagers.get(targetClass);

            if (delegationManager == null)
              throw new SerializationException("Cannot serialize " + obj
                  + " because there is no delegation manager which would provide " + targetClass.getName());

            return delegationManager.serializeWrapObject(target);
          }

          @Override
          public <I extends TBase<?, ?>, M extends TBase<?, ?>, O extends DataSerialization<M>> O deserializeChild(
              Class<? extends O> targetClass, I obj) throws DeserializationException {
            throw new UnsupportedOperationException();
          }
        };
      };

  private Map<Class<? extends TBase<?, ?>>, DataSerializationDelegationManager<?>> delegationManagers;

  /* package */ DataSerializer(Map<Class<? extends DataSerialization<?>>, Class<? extends TBase<?, ?>>> thriftClasses,
      Map<Class<? extends TBase<?, ?>>, DataSerializationDelegationManager<?>> delegationManagers) {
    this.thriftClasses = thriftClasses;
    this.delegationManagers = delegationManagers;
  }

  /**
   * Serialize a {@link DataSerialization} object into an output stream and flush that stream.
   * 
   * @param obj
   *          The object to serialize.
   * @param outputStream
   *          The output stream to fill.
   * @param objectDoneConsumer
   *          Will be called when single objects (referenced transitively from obj) have been "serialized" and can be
   *          freed by the caller, if needed.
   * @throws SerializationException
   *           If anything went wrong.
   */
  public void serialize(DataSerialization<?> obj, OutputStream outputStream, ObjectDoneConsumer objectDoneConsumer)
      throws SerializationException {
    DataSerializationHelper helper = dataSerializationHelperFactory.apply(objectDoneConsumer);
    TBase<?, ?> res = helper.serializeChild(thriftClasses.get(obj.getClass()), obj);
    TIOStreamTransport transport = new TIOStreamTransport(outputStream);
    TProtocol compactProt = new TCompactProtocol(transport);
    try {
      res.write(compactProt);
      outputStream.flush();
    } catch (TException | IOException e) {
      throw new SerializationException("Could not serialize", e);
    }
  }

  /**
   * Consumes objects that have been serialized already fully.
   */
  public static interface ObjectDoneConsumer extends Consumer<DataSerialization<?>> {
  }
}
