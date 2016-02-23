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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.consensus.ConsensusStateMachineManager;
import org.diqube.context.AutoInstatiate;

import com.google.common.collect.Sets;

import io.atomix.catalyst.serializer.JdkTypeResolver;
import io.atomix.catalyst.serializer.PrimitiveTypeResolver;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Catalyst serializer used by diqube.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeCatalystSerializer extends Serializer {
  private static final int BASE_SERIALIZATION_ID = 2500;

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

      // start suing IDs at an arbitrary, but fixed point, so we do not overwrite IDs used internally by copycat.
      int nextId = BASE_SERIALIZATION_ID;
      for (Class<?> opClass : serializationClassesSorted) {
        registry.register(opClass, nextId++);
      }
    });
  }
}
