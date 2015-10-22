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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel.MapMode;
import java.util.Collection;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.column.AdjustableStandardColumnShard;
import org.diqube.data.column.StandardColumnShard;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.table.TableShard;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileReader;
import org.diqube.util.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads data from .diqube files, which contain serialized data of classes from diqube-data (built with diqube-file) -
 * these simply have to be deserialized.
 * 
 * <p>
 * This loader will return as many {@link TableShard}s as contained in the .diqube file.
 * 
 * <p>
 * The corresponding files can be created using diqube-tool (transpose) or diqube-hadoop.
 * 
 * <p>
 * This {@link Loader} ignores the {@link LoaderColumnInfo} that is provided completely, but rather fully relies on the
 * serialized data.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeLoader implements Loader {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeLoader.class);

  @Inject
  private DiqubeFileFactory fileFactory;

  @Override
  public Collection<TableShard> load(long firstRowId, String filename, String tableName, LoaderColumnInfo columnInfo)
      throws LoadException {
    logger.info("Reading data for new table '{}' from '{}'.", new Object[] { tableName, filename });

    try (RandomAccessFile f = new RandomAccessFile(filename, "r")) {
      BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load());

      return load(firstRowId, buf, tableName, columnInfo);
    } catch (IOException e) {
      throw new LoadException("Could not load " + filename, e);
    }
  }

  @Override
  public Collection<TableShard> load(long firstRowId, BigByteBuffer buffer, String tableName,
      LoaderColumnInfo columnInfo) throws LoadException {
    Collection<TableShard> tableShards;
    try {
      DiqubeFileReader reader = fileFactory.createDiqubeFileReader(buffer);
      logger.info("Loading data for table '{}' by deserializing it.", tableName);
      tableShards = reader.loadAllTableShards();
    } catch (DeserializationException | IOException e) {
      throw new LoadException("Could not deserialize data", e);
    }

    long nextFirstRowId = firstRowId;
    // adjust some data.
    for (TableShard shard : tableShards) {
      shard.setTableName(tableName);
      for (StandardColumnShard colShard : shard.getColumns().values())
        ((AdjustableStandardColumnShard) colShard).adjustToFirstRowId(nextFirstRowId);
      nextFirstRowId += shard.getNumberOfRowsInShard();
    }

    logger.info("Successfully loaded data for table '{}', rowIds {}-{}.", tableName, firstRowId, nextFirstRowId - 1);

    return tableShards;
  }

}
