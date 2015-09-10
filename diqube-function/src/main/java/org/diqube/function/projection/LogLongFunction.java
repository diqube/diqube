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

import org.apache.commons.math3.util.FastMath;
import org.diqube.data.ColumnType;
import org.diqube.function.Function;

/**
 * Calculate logarithm to base e of a long.
 *
 * @author Bastian Gloeckle
 */
@Function(name = LogLongFunction.NAME)
public class LogLongFunction extends AbstractSingleParamProjectionFunction<Long, Double> {

  public static final String NAME = "log";

  public LogLongFunction() {
    super(NAME, ColumnType.LONG, ColumnType.DOUBLE, LogLongFunction::log);
  }

  public static double log(Long l) {
    return FastMath.log(l);
  }
}
