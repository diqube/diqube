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
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.thrift.TBase;
import org.diqube.context.AutoInstatiate;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * manager for all de-/serialization action on diqube-data data.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DataSerializationManager {
  private static final String BASE_PKG = "org.diqube.data";

  /**
   * Map from data class (diqube-data) to the thrift class it maps to (according to all annotations)
   */
  private Map<Class<? extends DataSerialization<?>>, Class<? extends TBase<?, ?>>> thriftClasses = new HashMap<>();

  /**
   * Map from thrift class to data class (diqube-data) it maps to (according to all annotations)
   */
  private Map<Class<? extends TBase<?, ?>>, Class<? extends DataSerialization<?>>> liveClasses = new HashMap<>();

  /**
   * Map from thrift class to {@link DataSerializationDelegationManager} defined at its annotation.
   */
  private Map<Class<? extends TBase<?, ?>>, DataSerializationDelegationManager<?>> delegationManagers = new HashMap<>();

  public DataSerializer createSerializer(Class<? extends DataSerialization<?>> rootSerializationClass) {
    if (!thriftClasses.containsKey(rootSerializationClass))
      return null;
    return new DataSerializer(thriftClasses, delegationManagers);
  }

  public DataDeserializer createDeerializer() {
    return new DataDeserializer(thriftClasses, liveClasses, delegationManagers);
  }

  @PostConstruct
  public void initialize() {
    ImmutableSet<ClassInfo> classInfos;
    try {
      classInfos = ClassPath.from(this.getClass().getClassLoader()).getTopLevelClassesRecursive(BASE_PKG);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse ClassPath.");
    }

    for (ClassInfo classInfo : classInfos) {
      Class<?> clazz = classInfo.load();

      if (!DataSerialization.class.isAssignableFrom(clazz))
        continue;

      Deque<Class<?>> allClasses = new LinkedList<>();
      allClasses.add(clazz);
      while (!allClasses.isEmpty()) {
        Class<?> curClazz = allClasses.pop();
        DataSerializable ann = curClazz.getDeclaredAnnotation(DataSerializable.class);
        if (ann != null) {
          @SuppressWarnings("unchecked")
          Class<? extends DataSerialization<?>> datSerClazz = (Class<? extends DataSerialization<?>>) clazz;

          Class<? extends TBase<?, ?>> thriftClass = ann.thriftClass();
          Class<? extends DataSerializationDelegationManager<?>> delegationManagerClass =
              ann.deserializationDelegationManager();

          thriftClasses.put(datSerClazz, thriftClass);
          liveClasses.put(thriftClass, datSerClazz);

          if (!delegationManagerClass.equals(DataSerializable.NONE.class)) {
            try {
              DataSerializationDelegationManager<?> delegationManager = delegationManagerClass.newInstance();

              delegationManagers.put(thriftClass, delegationManager);
            } catch (InstantiationException | IllegalAccessException e) {
              throw new RuntimeException("Could not instantiate " + delegationManagerClass);
            }
          }
          break;
        }
        if (curClazz.getSuperclass() != null && !curClazz.getSuperclass().equals(Object.class))
          allClasses.add(curClazz.getSuperclass());
        allClasses.addAll(Arrays.asList(curClazz.getInterfaces()));
      }
    }
  }
}
