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

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.diqube.data.serialize.DataSerialization.DataSerializationHelper;
import org.diqube.data.serialize.thrift.v1.SDiqubeData;
import org.diqube.data.serialize.thrift.v1.SDiqubeHeader;
import org.diqube.util.Pair;

/**
 * Deserializes thrift objects to {@link DataSerialization} objects.
 *
 * @author Bastian Gloeckle
 */
public class DataDeserializer {
  private static int HEADER_BYTES;

  private static final String THRIFT_PKG = "org.diqube.data.serialize.thrift.v1.";

  private Map<Class<? extends DataSerialization<?>>, Class<? extends TBase<?, ?>>> thriftClasses;
  private Map<Class<? extends TBase<?, ?>>, DataSerializationDelegationManager<?>> delegationManagers;
  private Map<Class<? extends TBase<?, ?>>, Class<? extends DataSerialization<?>>> liveClasses;

  private Function<Consumer<TBase<?, ?>>, DataSerializationHelper> dataSerializationHelperFactory =
      (cleanupConsumer) -> {
        return new DataSerializationHelper() {
          @Override
          public <M extends TBase<?, ?>, I extends DataSerialization<M>, O extends TBase<?, ?>> O serializeChild(
              Class<? extends O> targetClass, I obj) throws SerializationException {
            throw new UnsupportedOperationException();
          }

          @SuppressWarnings("unchecked")
          @Override
          public <I extends TBase<?, ?>, M extends TBase<?, ?>, O extends DataSerialization<M>> O deserializeChild(
              Class<? extends O> targetClass, I thriftObj) throws DeserializationException {
            if (thriftObj == null)
              return null;

            try {
              // check if we can deserialize into the given targetClass directly
              if (thriftClasses.containsKey(targetClass) && thriftClasses.get(targetClass).isInstance(thriftObj)
                  && !targetClass.isInterface() && !Modifier.isAbstract(targetClass.getModifiers())) {
                O res = targetClass.newInstance();
                M mObj = (M) thriftObj;
                res.deserialize(this, mObj);

                cleanupConsumer.accept(mObj);

                return res;
              }

              if (delegationManagers.containsKey(thriftObj.getClass())) {
                DataSerializationDelegationManager<I> delegationManager =
                    (DataSerializationDelegationManager<I>) delegationManagers.get(thriftObj.getClass());
                Pair<Class<? extends DataSerialization<?>>, TBase<?, ?>> delegateRes =
                    delegationManager.getDeserializationDelegate(thriftObj);

                DataSerialization<?> res = delegateRes.getLeft().newInstance();
                if (!targetClass.isInstance(res))
                  throw new DeserializationException("DelegationManager gave us " + delegateRes.getLeft().getName()
                      + " but " + targetClass.getName() + " was expected.");

                O oRes = (O) res;
                oRes.deserialize(this, (M) delegateRes.getRight());

                cleanupConsumer.accept(delegateRes.getRight());
                cleanupConsumer.accept(thriftObj);

                return oRes;
              }

              throw new DeserializationException("Object cannot be deserialized into expected type: "
                  + thriftObj.getClass().getName() + " expected: " + targetClass.getName());
            } catch (InstantiationException | IllegalAccessException e) {
              throw new DeserializationException("Cannot deserialize", e);
            }
          }
        };
      };

  /* package */ DataDeserializer(Map<Class<? extends DataSerialization<?>>, Class<? extends TBase<?, ?>>> thriftClasses,
      Map<Class<? extends TBase<?, ?>>, Class<? extends DataSerialization<?>>> liveClasses,
      Map<Class<? extends TBase<?, ?>>, DataSerializationDelegationManager<?>> delegationManagers) {
    this.thriftClasses = thriftClasses;
    this.liveClasses = liveClasses;
    this.delegationManagers = delegationManagers;
  }

  /**
   * Deserialize the data available in a byte array into a {@link DataSerialization} object hierarchy.
   * 
   * @throws DeserializationException
   *           if anything went wrong.
   */
  // we need to capture the generics here to make javac happy
  public <T extends TBase<?, ?>, M extends TBase<?, ?>, O extends DataSerialization<M>> O deserialize(byte[] data)
      throws DeserializationException {
    T thrift = deserializeToThrift(data);
    data = null;

    DataSerializationHelper helper = dataSerializationHelperFactory.apply(new Consumer<TBase<?, ?>>() {
      @Override
      public void accept(TBase<?, ?> t) {
        // cleanup action is to clear all field values to release memory as soon as possible.
        t.clear();
      }
    });

    @SuppressWarnings("unchecked")
    Class<? extends O> targetClz = (Class<? extends O>) liveClasses.get(thrift.getClass());
    O res = helper.deserializeChild(targetClz, thrift);
    return res;
  }

  private <T extends TBase<?, ?>> T deserializeToThrift(byte[] data) throws DeserializationException {
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    try {
      SDiqubeHeader header = new SDiqubeHeader();
      new TDeserializer(new TBinaryProtocol.Factory()).deserialize(header, data, 0, HEADER_BYTES);

      SDiqubeData diqubeData = new SDiqubeData();
      deserializer.deserialize(diqubeData, data, HEADER_BYTES, header.getDiqubeDataLength());

      if (header.getVersion() != DataSerializer.VERSION || !DataSerializer.MAGIC_STRING.equals(diqubeData.getMagic()))
        throw new DeserializationException("Invalid stream.");

      String thriftClassName = diqubeData.getSerializedClass();

      if (thriftClassName.contains("."))
        throw new DeserializationException("Invalid thrift classname.");

      @SuppressWarnings("unchecked")
      Class<? extends TBase<?, ?>> thriftClass =
          (Class<? extends TBase<?, ?>>) Class.forName(THRIFT_PKG + thriftClassName);

      @SuppressWarnings("unchecked")
      T thrift = (T) thriftClass.newInstance();
      deserializer.deserialize(thrift, data, HEADER_BYTES + header.getDiqubeDataLength(),
          data.length - (HEADER_BYTES + header.getDiqubeDataLength()));

      return thrift;
    } catch (TException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new DeserializationException("Cannot deserialize", e);
    }

  }

  static {
    SDiqubeHeader header = new SDiqubeHeader();
    header.setVersion(1);
    header.setDiqubeDataLength(1);
    try {
      HEADER_BYTES = new TSerializer(new TBinaryProtocol.Factory()).serialize(header).length;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
