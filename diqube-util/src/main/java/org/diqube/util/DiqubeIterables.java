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

import java.util.Iterator;

import com.google.common.collect.Iterables;

/**
 * Similar to {@link Iterables} but with custom methods.
 *
 * @author Bastian Gloeckle
 */
public class DiqubeIterables {
  /**
   * Checks if iterable iterable1 starts with all elements of iterable iterable2.
   */
  public static boolean startsWith(Iterable<?> iterable1, Iterable<?> iterable2) {
    Iterator<?> it1 = iterable1.iterator();
    Iterator<?> it2 = iterable2.iterator();
    while (it2.hasNext()) {
      Object next2 = it2.next();
      if (!it1.hasNext())
        return false;
      Object next1 = it1.next();
      if (!next1.equals(next2))
        return false;
    }
    return true;
  }
}
