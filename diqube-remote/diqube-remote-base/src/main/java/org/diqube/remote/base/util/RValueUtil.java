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
package org.diqube.remote.base.util;

import org.diqube.remote.base.thrift.RValue;

/**
 *
 * @author Bastian Gloeckle
 */
public class RValueUtil {
  public static RValue createRValue(Object value) {
    RValue res = new RValue();
    if (value instanceof String)
      res.setStrValue((String) value);
    else if (value instanceof Long)
      res.setLongValue((Long) value);
    else if (value instanceof Double)
      res.setDoubleValue((Double) value);
    else
      return null;
    return res;
  }

  public static Object createValue(RValue value) {
    Object res;
    if (value.isSetStrValue())
      res = value.getStrValue();
    else if (value.isSetLongValue())
      res = value.getLongValue();
    else
      res = value.getDoubleValue();
    return res;
  }
}
