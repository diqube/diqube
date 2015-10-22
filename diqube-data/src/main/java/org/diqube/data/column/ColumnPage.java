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
package org.diqube.data.column;

import org.diqube.data.serialize.DataSerialization;
import org.diqube.data.serialize.thrift.v1.SColumnPage;
import org.diqube.data.types.lng.array.CompressedLongArray;
import org.diqube.data.types.lng.dict.LongDictionary;

/**
 * {@link ColumnPage} holds the data of a specific set of consecutive rows of one {@link ColumnShard}.
 * 
 * <p>
 * A {@link ColumnPage} contains a 'Column Page Dictionary' which acts similar to the 'Column Dictionary' which is
 * defined in {@link ColumnShard}: It maps the Column Value IDs of the values stored in this {@link ColumnPage} to
 * 'Column Page Value IDs'. These 'Column Page Value IDs' are then available in an array, where there is one entry for
 * each row stored by this {@link ColumnPage}. These entries in the column page value array can then be mapped to column
 * value IDs using the Column Page Dictionary and can then in turn be mapped to uncompressed values using the Column
 * Dictionary.
 *
 * @author Bastian Gloeckle
 */
public interface ColumnPage extends DataSerialization<SColumnPage> {

  /**
   * @return The column page dictionary. See interface comment for details.
   */
  public LongDictionary<?> getColumnPageDict();

  /**
   * @return The array containing one "column page value id" for each row stored in this {@link ColumnPage}.
   */
  public CompressedLongArray<?> getValues();

  /**
   * @return The ID of the first row in this {@link ColumnPage}.
   */
  public long getFirstRowId();

  /**
   * @return Number of rows of which this {@link ColumnPage} contains values.
   */
  public int size();

  /**
   * @return Name of the {@link ColumnPage}.
   */
  public String getName();

  /**
   * @return An approximate number of bytes taken up by this {@link ColumnPage}. Note that this is only an
   *         approximation!
   */
  public long calculateApproximateSizeInBytes();

}
