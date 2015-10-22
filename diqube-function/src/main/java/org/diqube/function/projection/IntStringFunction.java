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
package org.diqube.function.projection;

import org.diqube.data.column.ColumnType;
import org.diqube.function.Function;
import org.diqube.function.FunctionException;

/**
 * Parses a string into a long.
 * 
 * This is for convenience, because users might be more used to using "int" to convert a string to a number format than
 * using "long". This implementation is totally equivalent to {@link LongStringFunction}.
 *
 * @author Bastian Gloeckle
 */
@Function(name = IntStringFunction.NAME)
public class IntStringFunction extends AbstractSingleParamProjectionFunction<String, Long> {
  public static final String NAME = "int";

  public IntStringFunction() {
    super(NAME, ColumnType.STRING, ColumnType.LONG, IntStringFunction::parse);
  }

  public static Long parse(String s) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      throw new FunctionException(s + " cannot be parsed to an int.");
    }
  }
}
