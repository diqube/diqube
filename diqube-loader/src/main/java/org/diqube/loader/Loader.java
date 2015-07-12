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
package org.diqube.loader;

import org.diqube.data.TableShard;
import org.diqube.util.BigByteBuffer;

/**
 * A {@link Loader} is capable of loading data from a file or a stream to build a new {@link TableShard} from it.
 *
 * <p>
 * Each implementation of this interface supports loading from a different data format.
 *
 * @author Bastian Gloeckle
 */
public interface Loader {

  /**
   * Load a {@link TableShard} from a {@link BigByteBuffer}.
   * 
   * <p>
   * Be aware that this method might take a while to finish.
   * 
   * @param firstRowId
   *          The rowId to be used for the first "row" of data in the buffer. This will be the first rowId where the
   *          returned TableShard has data. Note that each TableShard contains data for consecutive rowIds - therefore
   *          the returned TableShard will contain data for the rowIds firstRowId..(firstRowId+number of rows-1). Each
   *          rowId must be mapped only in one TableShard of a Table, the overall first rowId in a Table must be 0L and
   *          the rowIds for all rows in the table must be consecutive.
   * @param buffer
   *          The buffer which contains the raw data in a format that this Loader supports.
   * @param tableName
   *          Name of the table the returned {@link TableShard} should belong to.
   * @param columnInfo
   *          additional information about the columns to be loaded. Depending on the implementation of the Loader, the
   *          Loader itself might be able to identify the data type of specific columns pretty well itself - some though
   *          do not.
   * 
   * @return The newly loaded TableShard.
   * @throws LoadException
   *           If the {@link TableShard} cannot be created for some reason.
   */
  public TableShard load(long firstRowId, BigByteBuffer buffer, String tableName, LoaderColumnInfo columnInfo)
      throws LoadException;

  /**
   * Load a {@link TableShard} from a file.
   * 
   * <p>
   * Be aware that this method might take a while to finish.
   * 
   * @param firstRowId
   *          The rowId to be used for the first "row" of data in the buffer. This will be the first rowId where the
   *          returned TableShard has data. Note that each TableShard contains data for consecutive rowIds - therefore
   *          the returned TableShard will contain data for the rowIds firstRowId..(firstRowId+number of rows-1). Each
   *          rowId must be mapped only in one TableShard of a Table, the overall first rowId in a Table must be 0L and
   *          the rowIds for all rows in the table must be consecutive.
   * @param tableName
   *          Name of the table the returned {@link TableShard} should belong to.
   * @param columnInfo
   *          additional information about the columns to be loaded. Depending on the implementation of the Loader, the
   *          Loader itself might be able to identify the data type of specific columns pretty well itself - some though
   *          do not.
   * @param buffer
   *          The buffer which contains the raw data in a format that this Loader supports.
   * @return The newly loaded TableShard.
   * @throws LoadException
   *           If the {@link TableShard} cannot be created for some reason.
   */
  public TableShard load(long firstRowId, String filename, String tableName, LoaderColumnInfo columnInfo)
      throws LoadException;
}
