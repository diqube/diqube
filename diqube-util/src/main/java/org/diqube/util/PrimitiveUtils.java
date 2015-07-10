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

/**
 *
 * @author Bastian Gloeckle
 */
public class PrimitiveUtils {
  public static Long[] toBoxedArray(long[] longArray) {
    Long[] res = new Long[longArray.length];
    for (int i = 0; i < res.length; i++)
      res[i] = longArray[i];
    return res;
  }

  public static Double[] toBoxedArray(double[] doubleArray) {
    Double[] res = new Double[doubleArray.length];
    for (int i = 0; i < res.length; i++)
      res[i] = doubleArray[i];
    return res;
  }

  public static double[] toUnboxedArray(Double[] doubleArray) {
    double[] res = new double[doubleArray.length];
    for (int i = 0; i < res.length; i++)
      res[i] = doubleArray[i];
    return res;
  }

  public static long[] toUnboxedArray(Long[] longArray) {
    long[] res = new long[longArray.length];
    for (int i = 0; i < res.length; i++)
      res[i] = longArray[i];
    return res;
  }
}
