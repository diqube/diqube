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

/**
 * Simple identity function
 *
 * @author Bastian Gloeckle
 */
@Function(name = IdStringFunction.NAME)
public class IdStringFunction extends AbstractSingleParamSameColTypeProjectionFunction<String> {

  public static final String NAME = "id";

  public IdStringFunction() {
    super(NAME, ColumnType.STRING, IdStringFunction::id);
  }

  public static String id(String a) {
    return a;
  }
}
