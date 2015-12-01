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
package org.diqube.consensus;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.diqube.context.AutoInstatiate;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

import io.atomix.copycat.client.Operation;
import io.atomix.copycat.server.Commit;

/**
 * Manages all interfaces/classes that have a {@link ConsensusStateMachine} annotation.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeConsensusStateMachineManager {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeConsensusStateMachineManager.class);
  private static final String BASE_PKG = "org.diqube";

  /**
   * Triple: Interface class, Method Name, Parameter classes of target method.
   */
  private Map<Class<? extends Operation<?>>, Triple<Class<?>, String, Class<?>[]>> dataClassToInterfaceAndMethodNameAndParameters;
  private Map<Class<?>, Class<?>> interfaceToImpl;
  private Map<Class<?>, Set<Class<? extends Operation<?>>>> interfaceToOperations;

  /**
   * @return All Copycat {@link Operation} classes that the central copycat state machine needs to support.
   */
  public Set<Class<? extends Operation<?>>> getAllOperationClasses() {
    return Collections.unmodifiableSet(dataClassToInterfaceAndMethodNameAndParameters.keySet());
  }

  /**
   * Details about the implementation of a specific {@link Operation} - what should a server call when a object of the
   * given class arrives?
   * 
   * @return {@link Pair} of {@link Class} object denoting the implementing class and {@link Method} that should be
   *         called.
   * @throws IllegalStateException
   *           If anything goes wrong.
   */
  public Pair<Class<?>, Method> getImplementation(Class<? extends Operation<?>> operationClass)
      throws IllegalStateException {
    Triple<Class<?>, String, Class<?>[]> t = dataClassToInterfaceAndMethodNameAndParameters.get(operationClass);

    Class<?> interfaceClass = t.getLeft();
    String methodName = t.getMiddle();
    Class<?>[] methodParameters = t.getRight();

    Class<?> implClass = interfaceToImpl.get(interfaceClass);

    Method m;
    try {
      m = implClass.getMethod(methodName, methodParameters);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new IllegalStateException("Could not find implementation method", e);
    }

    return new Pair<>(implClass, m);
  }

  /**
   * @return All the Operation classes defined by a specific interface class.
   */
  public Map<String, Class<? extends Operation<?>>> getOperationClassesAndMethodNamesOfInterface(
      Class<?> interfaceClass) {
    Map<String, Class<? extends Operation<?>>> res = new HashMap<>();

    for (Class<? extends Operation<?>> dataClass : interfaceToOperations.get(interfaceClass))
      res.put(dataClassToInterfaceAndMethodNameAndParameters.get(dataClass).getMiddle(), dataClass);

    return res;
  }

  @PostConstruct
  public void initialize() {
    ImmutableSet<ClassInfo> classInfos;
    try {
      classInfos = ClassPath.from(this.getClass().getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);
    } catch (IOException e) {
      throw new RuntimeException("Could not inspect classpath", e);
    }

    Set<Class<?>> stateMachineInterfaces = new HashSet<>();
    Set<Class<?>> stateMachineImplementations = new HashSet<>();

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();
      boolean isStateMachineInterface =
          clazz.isInterface() && clazz.getDeclaredAnnotation(ConsensusStateMachine.class) != null;
      boolean isStateMachineImplementation =
          !clazz.isInterface() && clazz.getDeclaredAnnotation(ConsensusStateMachineImplementation.class) != null;

      if (isStateMachineInterface)
        stateMachineInterfaces.add(clazz);
      else if (isStateMachineImplementation)
        stateMachineImplementations.add(clazz);
    }

    interfaceToImpl = new HashMap<>();
    for (Class<?> implClass : stateMachineImplementations)
      interfaceToImpl.put(findStateMachineInterface(implClass, stateMachineInterfaces), implClass);

    if (interfaceToImpl.keySet().size() != stateMachineInterfaces.size())
      throw new RuntimeException("There are StateMachine interfaces that do not have any implementation: "
          + Sets.difference(stateMachineInterfaces, interfaceToImpl.keySet()));

    dataClassToInterfaceAndMethodNameAndParameters = new HashMap<>();
    interfaceToOperations = new HashMap<>();

    Class<?>[] expectedParamTypes = new Class<?>[] { Commit.class };

    for (Class<?> interfaceClass : interfaceToImpl.keySet()) {
      interfaceToOperations.put(interfaceClass, new HashSet<>());
      for (Method m : interfaceClass.getMethods()) {
        if (Modifier.isStatic(m.getModifiers()))
          continue;

        ConsensusMethod consensusMethod = m.getAnnotation(ConsensusMethod.class);
        if (consensusMethod != null) {
          if (!Arrays.equals(expectedParamTypes, m.getParameterTypes()))
            throw new RuntimeException("Method '" + m.toString() + "' has wrong parameter types to be a "
                + ConsensusMethod.class.getSimpleName());

          Class<? extends Operation<?>> dataClass = consensusMethod.dataClass();
          dataClassToInterfaceAndMethodNameAndParameters.put(dataClass,
              new Triple<Class<?>, String, Class<?>[]>(interfaceClass, m.getName(), m.getParameterTypes()));
          interfaceToOperations.get(interfaceClass).add(dataClass);
        }
      }
    }

    logger.debug("Loaded {} consensus operations of {} interfaces",
        dataClassToInterfaceAndMethodNameAndParameters.keySet().size(), interfaceToImpl.keySet().size());
  }

  private Class<?> findStateMachineInterface(Class<?> stateMachineImplementation,
      Set<Class<?>> stateMachineInterfaces) {
    Set<Class<?>> interfaces = new HashSet<>();
    Class<?> curClass = stateMachineImplementation;
    while (curClass != Object.class) {
      interfaces.addAll(Arrays.asList(curClass.getInterfaces()));
      curClass = curClass.getSuperclass();
    }

    Set<Class<?>> curStateMachineInterfaces = Sets.intersection(interfaces, stateMachineInterfaces);

    if (curStateMachineInterfaces.isEmpty())
      throw new RuntimeException(
          "Class " + stateMachineImplementation.getName() + " seems to not implement any StateMachineInterfaces.");

    if (curStateMachineInterfaces.size() > 1)
      throw new RuntimeException("Class " + stateMachineImplementation.getName()
          + " seems to implement multiple StateMachineInterfaces: " + curStateMachineInterfaces.toString());

    return curStateMachineInterfaces.iterator().next();
  }
}
