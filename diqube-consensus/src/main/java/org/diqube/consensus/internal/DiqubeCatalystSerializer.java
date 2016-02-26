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
package org.diqube.consensus.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.consensus.ConsensusStateMachineManager;
import org.diqube.context.AutoInstatiate;

import com.google.common.collect.Sets;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.buffer.OutputStreamBufferOutput;
import io.atomix.catalyst.serializer.JdkTypeResolver;
import io.atomix.catalyst.serializer.PrimitiveTypeResolver;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.serializer.util.JavaSerializableSerializer;

/**
 * Catalyst serializer used by diqube.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCatalystSerializer extends Serializer {
  private static final int BASE_SERIALIZATION_ID = 2500;

  /**
   * Additional classes that we need to register in order to serialize them correctly.
   */
  private static final Class<?>[] ADDITIONAL_SERIALIZATION_CLASSES = { //
      // IllegalStateException is send e.g. by InactiveState if a client tries to communicate with an inactive server -
      // be sure that the server response is received by the client and it can retry.
      IllegalStateException.class //
  };

  @Inject
  private ConsensusStateMachineManager consensusStateMachineManager;

  public DiqubeCatalystSerializer() {
    super(new PrimitiveTypeResolver(), new JdkTypeResolver());
  }

  @PostConstruct
  public void initialize() {
    // register all the operation classes in the serializer so they can be serialized (we need to whitelist them).
    // Register all additional classes, too.
    this.resolve((registry) -> {
      Set<Class<?>> allSerializationClasses = Sets.union(consensusStateMachineManager.getAllOperationClasses(),
          consensusStateMachineManager.getAllAdditionalSerializationClasses());
      List<Class<?>> serializationClassesSorted = allSerializationClasses.stream()
          .sorted((c1, c2) -> c1.getName().compareTo(c2.getName())).collect(Collectors.toList());

      serializationClassesSorted.addAll(Arrays.asList(ADDITIONAL_SERIALIZATION_CLASSES));

      // start suing IDs at an arbitrary, but fixed point, so we do not overwrite IDs used internally by copycat.
      int nextId = BASE_SERIALIZATION_ID;
      for (Class<?> opClass : serializationClassesSorted) {
        registry.register(opClass, nextId++, DiqubeJavaSerializableSerializer.class);
      }
    });
  }

  /**
   * Helper method to validate an object can be sent as parameter for the consensus client.
   * 
   * TODO #107: Remove this.
   * 
   * @throws IllegalArgumentException
   *           If object is invalid.
   */
  public void validateSerializationObject(Object o) throws IllegalArgumentException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      new DiqubeJavaSerializableSerializer<>().write(o, new OutputStreamBufferOutput(baos), this);
    } catch (IOException | SerializationException e) {
      throw new IllegalArgumentException("Object invalid", e);
    }
  }

  /**
   * As long as catalysts {@link JavaSerializableSerializer} is buggy, we use this fixed implementation.
   * 
   * TODO #107: remove workaround.
   */
  public static class DiqubeJavaSerializableSerializer<T> implements TypeSerializer<T> {
    private static final int MAX_UNSIGNED_SHORT = (1 << 16) - 1;

    @SuppressWarnings("rawtypes")
    @Override
    public void write(T object, BufferOutput buffer, Serializer serializer) {
      try (ByteArrayOutputStream os = new ByteArrayOutputStream();
          ObjectOutputStream out = new ObjectOutputStream(os)) {
        out.writeObject(object);
        out.flush();
        byte[] bytes = os.toByteArray();

        // Workaround for copycat #173: The copycat Log uses an unsigned short length field, too.
        if (bytes.length > MAX_UNSIGNED_SHORT)
          throw new SerializationException("Cannot serialize java object because it is too big.");

        // Workaround for catalyst #30: Write "int", not "unsigned short"
        buffer.writeInt(bytes.length).write(bytes);
      } catch (IOException e) {
        throw new SerializationException("failed to serialize Java object", e);
      }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public T read(Class<T> type, BufferInput buffer, Serializer serializer) {
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        try {
          return (T) in.readObject();
        } catch (ClassNotFoundException e) {
          throw new SerializationException("failed to deserialize Java object", e);
        }
      } catch (IOException e) {
        throw new SerializationException("failed to deserialize Java object", e);
      }
    }
  }

}
