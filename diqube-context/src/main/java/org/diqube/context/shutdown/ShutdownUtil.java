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
package org.diqube.context.shutdown;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.util.TopologicalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Utility class to handle calls of {@link ContextShutdownListener} of a {@link ApplicationContext} just before
 * {@link ConfigurableApplicationContext#close() closing} it.
 *
 * @author Bastian Gloeckle
 */
public class ShutdownUtil {
  private static final Logger logger = LoggerFactory.getLogger(ShutdownUtil.class);

  private ApplicationContext context;

  public ShutdownUtil(ApplicationContext context) {
    this.context = context;
  }

  /**
   * Call {@link ContextShutdownListener#contextAboutToShutdown()} of all interested beans in a way that honors
   * {@link ShutdownAfter} and {@link ShutdownBefore} annotations.
   */
  public void callShutdownListeners() {
    List<ContextShutdownListener> allListeners = context.getBeansOfType(ContextShutdownListener.class).values().stream()
        .sorted((b1, b2) -> b1.getClass().getName().compareTo(b2.getClass().getName())).collect(Collectors.toList());

    // find the single method in ConextShutdownListener - its the one that is not available in Object. We will inspect
    // that method for the annotation!
    Method shutdownMethod = Stream.of(ContextShutdownListener.class.getMethods())
        .filter(m -> getMethodOrNull(Object.class, m.getName(), m.getParameterTypes()) == null).findAny().get();

    Map<Integer, List<Integer>> successors = new HashMap<>();

    // inspect annotations and find the classes which have to be shut down BEFORE a specific one.
    Map<Integer, List<Integer>> predecessors = new HashMap<>();
    for (ContextShutdownListener listener : allListeners) {
      try {
        Method curMethod = listener.getClass().getMethod(shutdownMethod.getName(), shutdownMethod.getParameterTypes());
        ShutdownAfter afterAnnotation = curMethod.getAnnotation(ShutdownAfter.class);

        List<Integer> beforeIndices = new ArrayList<>();

        if (afterAnnotation != null) {
          for (Class<?> beforeCls : afterAnnotation.value()) {
            if (!(ContextShutdownListener.class.isAssignableFrom(beforeCls))) {
              logger.warn("Class '{}' wanted to be shut down after '{}', but '{}' is no {}.",
                  listener.getClass().getName(), beforeCls.getName(), beforeCls.getName(),
                  ContextShutdownListener.class.getSimpleName());
              continue;
            }

            for (int i = 0; i < allListeners.size(); i++)
              if (beforeCls.isAssignableFrom(allListeners.get(i).getClass()))
                beforeIndices.add(i);
          }
        }
        predecessors.put(allListeners.indexOf(listener), beforeIndices);

        ShutdownBefore beforeAnnotation = curMethod.getAnnotation(ShutdownBefore.class);
        if (beforeAnnotation != null) {
          List<Integer> afterIndices = new ArrayList<>();
          for (Class<?> afterClass : beforeAnnotation.value()) {
            if (!(ContextShutdownListener.class.isAssignableFrom(afterClass))) {
              logger.warn("Class '{}' wanted to be shut down before '{}', but '{}' is no {}.",
                  listener.getClass().getName(), afterClass.getName(), afterClass.getName(),
                  ContextShutdownListener.class.getSimpleName());
              continue;
            }

            for (int i = 0; i < allListeners.size(); i++)
              if (afterClass.isAssignableFrom(allListeners.get(i).getClass()))
                afterIndices.add(i);
          }
          successors.put(allListeners.indexOf(listener), afterIndices);
        }
      } catch (NoSuchMethodException e) {
        // swallow, cannot happen, since all ContextShutdownListener s MUST have the shutdownMethod.
      }
    }

    // Turn around the predecessors and find out which classes need to be shut down AFTER a specific one.
    for (Entry<Integer, List<Integer>> predEntry : predecessors.entrySet()) {
      for (int beforeIdx : predEntry.getValue()) {
        if (!successors.containsKey(beforeIdx))
          successors.put(beforeIdx, new ArrayList<>());
        successors.get(beforeIdx).add(predEntry.getKey());
      }
    }

    // exeucte Topological sort on the classes, so we will execute the shutdown in the right order.
    TopologicalSort<ContextShutdownListener> topSort = new TopologicalSort<>(listener -> {
      int idx = allListeners.indexOf(listener);
      if (!successors.containsKey(idx))
        return new ArrayList<>();
      return successors.get(idx).stream().map(startIdx -> allListeners.get(startIdx)).collect(Collectors.toList());
    } , listener -> (long) allListeners.indexOf(listener), null);

    List<ContextShutdownListener> sortedListeners = topSort.sort(allListeners);

    // exeucte shutdown.
    for (ContextShutdownListener listener : sortedListeners)
      listener.contextAboutToShutdown();
  }

  private Method getMethodOrNull(Class<?> clazz, String methodName, Class<?>... methodParameterTypes) {
    try {
      return clazz.getMethod(methodName, methodParameterTypes);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
