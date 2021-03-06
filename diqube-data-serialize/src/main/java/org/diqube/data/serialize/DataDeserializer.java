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
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.diqube.data.serialize.DataSerialization.DataSerializationHelper;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes thrift objects to {@link DataSerialization} objects.
 * 
 * Does not expect any header/metainformation/etc in the stream, but only serialized thrift objects.
 *
 * An instance of this class can be re-used.
 *
 * @author Bastian Gloeckle
 */
public class DataDeserializer {
  private static final Logger logger = LoggerFactory.getLogger(DataDeserializer.class);

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
   * @param inputStream
   *          the stream containign the data to be deserialized. It will be tried to close this input stream as soon as
   *          possible to free any resources.
   * @throws DeserializationException
   *           if anything went wrong.
   */
  // we need to capture the generics here to make javac happy
  public <T extends TBase<?, ?>, M extends TBase<?, ?>, O extends DataSerialization<M>> O deserialize(
      Class<? extends O> targetClass, InputStream inputStream) throws DeserializationException {
    logger.trace("Deserializing to thrift...");
    @SuppressWarnings("unchecked")
    T thrift = deserializeToThrift(inputStream, (Class<? extends T>) thriftClasses.get(targetClass));
    try {
      inputStream.close();
    } catch (IOException e) {
      // swallow.
    }
    inputStream = null;

    DataSerializationHelper helper = dataSerializationHelperFactory.apply(new Consumer<TBase<?, ?>>() {
      @Override
      public void accept(TBase<?, ?> t) {
        // cleanup action is to clear all field values to release memory as soon as possible.
        t.clear();
      }
    });

    logger.trace("Transforming into final objects...");
    @SuppressWarnings("unchecked")
    Class<? extends O> targetClz = (Class<? extends O>) liveClasses.get(thrift.getClass());
    O res = helper.deserializeChild(targetClz, thrift);
    logger.trace("Deserialization done.");
    return res;
  }

  private <T extends TBase<?, ?>> T deserializeToThrift(InputStream inputStream, Class<? extends T> thriftClass)
      throws DeserializationException {
    TIOStreamTransport transport = new TIOStreamTransport(inputStream);
    TProtocol compactProt = new TCompactProtocol(transport);
    try {
      T thrift = thriftClass.newInstance();
      thrift.read(compactProt);

      return thrift;
    } catch (TException | InstantiationException | IllegalAccessException e) {
      throw new DeserializationException("Cannot deserialize", e);
    }

  }
}
