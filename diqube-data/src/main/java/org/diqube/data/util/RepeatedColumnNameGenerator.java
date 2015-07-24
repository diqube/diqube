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
package org.diqube.data.util;

import org.diqube.context.AutoInstatiate;

/**
 * A class that can generate the column names of repeated columns.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class RepeatedColumnNameGenerator {
  /**
   * Generate the col name for one object in the repeated field.
   * 
   * @param baseName
   *          Name of the column.
   * @param index
   *          the index for which to create the col name.
   * @return the column name.
   */
  public String repeatedAtIndex(String baseName, int index) {
    StringBuilder sb = new StringBuilder();
    sb.append(baseName);
    sb.append("[");
    sb.append(index);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Generate the col name for a repeated field that contains the number of entries this repeated field has.
   * 
   * @param baseName
   *          Name of the repeated field.
   * @return The colname whose col contains the length
   */
  public String repeatedLength(String baseName) {
    return baseName + "[length]";
  }
}
