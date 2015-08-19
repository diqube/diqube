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

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.AdjustableStandardColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.serialize.DataDeserializer;
import org.diqube.data.serialize.DataSerializationManager;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.util.BigByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads data from .diqube files, which contain serialized data of classes from diqube-data - these simply have to be
 * deserialized.
 * 
 * The corresponding files can be created using diqube-transpose.
 * 
 * This {@link Loader} ignores {@link LoaderColumnInfo} that is provided completely, but rather fully relies on the
 * serialized data.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class DiqubeLoader implements Loader {
  private static final Logger logger = LoggerFactory.getLogger(DiqubeLoader.class);

  @Inject
  private DataSerializationManager serializationManager;

  @Override
  public TableShard load(long firstRowId, String filename, String tableName, LoaderColumnInfo columnInfo)
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
  public TableShard load(long firstRowId, BigByteBuffer buffer, String tableName, LoaderColumnInfo columnInfo)
      throws LoadException {
    TableShard tableShard;
    try {
      DataDeserializer deserializer = serializationManager.createDeserializer();
      logger.info("Loading data for new table '{}' by deserializing it.", tableName);
      tableShard = deserializer.deserialize(buffer.createInputStream());
    } catch (DeserializationException e) {
      throw new LoadException("Could not deserialize data", e);
    }

    // adjust some data.
    tableShard.setTableName(tableName);
    for (StandardColumnShard colShard : tableShard.getColumns().values())
      ((AdjustableStandardColumnShard) colShard).adjustToFirstRowId(firstRowId);

    logger.info("Successfully loaded data for new table '{}'.", tableName);

    return tableShard;
  }

}
