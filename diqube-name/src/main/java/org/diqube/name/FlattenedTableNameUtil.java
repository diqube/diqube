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

import java.util.UUID;

import org.diqube.context.AutoInstatiate;

/**
 * Utility for handling table names of flattened tables.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class FlattenedTableNameUtil {
  private static final String FLATTEN_FN = "flatten";

  /**
   * @return Name of the table that has already been flattened by the given field and the flattening is available under
   *         the given flattenId.
   */
  public String createFlattenedTableName(String inputTableName, String flattenByField, UUID flattenId) {
    return FLATTEN_FN + "(" + inputTableName + "," + flattenByField + "," + flattenId + ")";
  }

  /**
   * @return Name of a flattened table in the form that is used in Diql queries - it misses a flattenId. Ensure that the
   *         target method can handle a table name created by this method, usually
   *         {@link #createFlattenedTableName(String, String, UUID)} has to be used!
   */
  public String createIncompleteFlattenedTableName(String inputTableName, String flattenByField) {
    return FLATTEN_FN + "(" + inputTableName + "," + flattenByField + ")";
  }

  public boolean isFlattenedTableName(String inputTableName) {
    return inputTableName.startsWith(FLATTEN_FN + "(");
  }

  public String getOriginalTableNameFromFlatten(String flattenedTableName) {
    return flattenedTableName.substring(FLATTEN_FN.length() + 1, flattenedTableName.indexOf(','));
  }

  /**
   * @return true if the provided flattened table name is a "full" one, i.e. one including the flattenId = one created
   *         by {@link #createFlattenedTableName(String, String, UUID)}. <code>false</code> is returned, if the
   *         flattenId is missing and the provided table name therefore is more like a table name used in diql to
   *         represent the newest flattened version of a table.
   */
  public boolean isFullFlattenedTableName(String flattenedTableName) {
    return flattenedTableName.chars().filter(c -> c == ',').count() == 2;
  }
}
