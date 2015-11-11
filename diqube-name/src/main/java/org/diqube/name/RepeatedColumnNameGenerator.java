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
package org.diqube.name;

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
  public String repeatedAtIndex(String baseName, long index) {
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
    return baseName + lengthIdentifyingSuffix();
  }

  /**
   * @return Suffix of a col name if that col name is a "length" column.
   */
  public String lengthIdentifyingSuffix() {
    return "[length]";
  }

  /**
   * @return A substring that can be appended to a column when used in a query name to denote that the column is
   *         repeated and all children are referenced. Strings containing this substring are called "patterns".
   * @see #allEntriesManifestedSubstr()
   */
  public String allEntriesIdentifyingSubstr() {
    return "[*]";
  }

  /**
   * @return A substring that is contained in column names that were built by a query that contained
   *         {@link #allEntriesIdentifyingSubstr()}, i.e. that aggregated/projected over columns in a row. This
   *         substring is used instead of "[*]" in the output column of those aggregation/projection columns. Using this
   *         "manifested" string is needed, if the output column is used in a consecutive step which would, if [*] is
   *         still used, again try to aggregate/project columns.
   */
  public String allEntriesManifestedSubstr() {
    return "[a]";
  }
}
