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
package org.diqube.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.util.ReflectionUtils;

/**
 * Utility which can set all properties to <code>null</code> on an object.
 *
 * @author Bastian Gloeckle
 */
public class NullUtil {
  private static final Set<Class<?>> PRIMITIVE_TYPES = new HashSet<>(Arrays.asList(Long.TYPE, Integer.TYPE, Byte.TYPE,
      Short.TYPE, Float.TYPE, Double.TYPE, Character.TYPE, Boolean.TYPE));

  /**
   * Set all properties to <code>null</code> on the given object, enabling the GC to clean up some objects.
   * 
   * @param o
   *          The object to <code>null</code> the properties of
   * @param exceptionHandler
   *          Called when an excpetion occurs. If <code>null</code> exceptions will be swallowed. Consumes the fieldName
   *          and the exception that happened.
   */
  public static void setAllPropertiesToNull(Object o, BiConsumer<String, Exception> exceptionHandler) {
    List<Field> allFields = new ArrayList<>();
    Deque<Class<?>> allClasses = new LinkedList<>();
    allClasses.add(o.getClass());
    while (!allClasses.isEmpty()) {
      Class<?> curClass = allClasses.pop();
      if (curClass.equals(Object.class))
        continue;

      for (Field declaredField : curClass.getDeclaredFields()) {
        if (!PRIMITIVE_TYPES.contains(declaredField.getType()))
          allFields.add(declaredField);
      }
      allClasses.add(curClass.getSuperclass());
    }

    for (Field fieldToNull : allFields) {
      ReflectionUtils.makeAccessible(fieldToNull);
      try {
        fieldToNull.set(o, null);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        if (exceptionHandler != null)
          exceptionHandler.accept(fieldToNull.getName(), e);
      }
    }
  }
}
