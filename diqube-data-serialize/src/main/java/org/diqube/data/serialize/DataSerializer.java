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
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.diqube.data.serialize.DataSerialization.DataSerializationHelper;
import org.diqube.data.serialize.thrift.v1.SDiqubeData;
import org.diqube.data.serialize.thrift.v1.SDiqubeHeader;

/**
 * Serializes from {@link DataSerialization} to thrift objects.
 *
 * @author Bastian Gloeckle
 */
public class DataSerializer {
  public static final String MAGIC_STRING = "DiTaDa";
  public static final int VERSION = 1;

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
   * Serialize a {@link DataSerialization} object into an output stream.
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
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    try {
      SDiqubeData diqubeData = new SDiqubeData();
      diqubeData.setMagic(MAGIC_STRING);
      diqubeData.setSerializedClass(res.getClass().getSimpleName());

      byte[] diqubeDataSerialized = serializer.serialize(diqubeData);

      SDiqubeHeader header = new SDiqubeHeader();
      header.setVersion(VERSION);
      header.setDiqubeDataLength(diqubeDataSerialized.length);

      outputStream.write(new TSerializer(new TBinaryProtocol.Factory()).serialize(header));
      outputStream.write(diqubeDataSerialized);
      outputStream.write(serializer.serialize(res));
      outputStream.flush();
    } catch (TException | IOException e) {
      throw new SerializationException("Could not serialize", e);
    }
  }

  public static interface ObjectDoneConsumer extends Consumer<DataSerialization<?>> {

  }
}
