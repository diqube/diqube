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

import java.util.function.Function;

import org.diqube.data.column.ColumnType;

/**
 * Abstract implementation for projection functions with a single param and which produce the same output
 * {@link ColumnType} as the input.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractSingleParamSameColTypeProjectionFunction<T>
    extends AbstractSingleParamProjectionFunction<T, T> {

  protected AbstractSingleParamSameColTypeProjectionFunction(String nameLowercase, ColumnType inputAndOutputType,
      Function<T, T> fn) {
    super(nameLowercase, inputAndOutputType, inputAndOutputType, fn);
  }
}
