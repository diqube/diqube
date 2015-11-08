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
 * Parses a string into a double.
 *
 * @author Bastian Gloeckle
 */
@Function(name = DoubleStringFunction.NAME)
public class DoubleStringFunction extends AbstractSingleParamProjectionFunction<String, Double> {
  public static final String NAME = "double";

  public DoubleStringFunction() {
    super(NAME, ColumnType.STRING, ColumnType.DOUBLE, DoubleStringFunction::parse);
  }

  public static Double parse(String s) {
    try {
      return Double.parseDouble(s);
    } catch (NumberFormatException e) {
      throw new FunctionException(s + " cannot be parsed to an int.");
    }
  }
}