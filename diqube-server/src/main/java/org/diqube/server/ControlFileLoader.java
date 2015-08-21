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
package org.diqube.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.diqube.data.ColumnType;
import org.diqube.data.Table;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.execution.TableRegistry;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.loader.Loader;
import org.diqube.loader.LoaderColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the table shard whose data is referred to by a .control file.
 *
 * @author Bastian Gloeckle
 */
public class ControlFileLoader {
  private static final Logger logger = LoggerFactory.getLogger(ControlFileLoader.class);

  public static final String KEY_FILE = "file";
  public static final String KEY_TYPE = "type";
  public static final String KEY_TABLE = "table";
  public static final String KEY_FIRST_ROWID = "firstRowId";
  public static final String KEY_COLTYPE_PREFIX = "columnType.";
  public static final String KEY_DEFAULT_COLTYPE = "defaultColumnType";

  public static final String TYPE_CSV = "csv";
  public static final String TYPE_JSON = "json";
  public static final String TYPE_DIQUBE = "diqube";

  private File controlFile;
  private TableRegistry tableRegistry;
  private TableFactory tableFactory;
  private CsvLoader csvLoader;
  private JsonLoader jsonLoader;
  private DiqubeLoader diqubeLoader;

  public ControlFileLoader(TableRegistry tableRegistry, TableFactory tableFactory, CsvLoader csvLoader,
      JsonLoader jsonLoader, DiqubeLoader diqubeLoader, File controlFile) {
    this.tableRegistry = tableRegistry;
    this.tableFactory = tableFactory;
    this.csvLoader = csvLoader;
    this.jsonLoader = jsonLoader;
    this.diqubeLoader = diqubeLoader;
    this.controlFile = controlFile;
  }

  private ColumnType resolveColumnType(String controlFileString) throws LoadException {
    try {
      return ColumnType.valueOf(controlFileString.toUpperCase());
    } catch (RuntimeException e) {
      throw new LoadException(controlFileString + " is no valid ColumnType.");
    }
  }

  /**
   * Loads the table shard synchronously.
   * 
   * <p>
   * This method will automatically retry loading the .control file if it fails - this is needed if the control file has
   * not been written completely yet. If the validation/loading of the control file still fails after a few attempts, a
   * {@link LoadException} will be thrown.
   * 
   * @return The name of the table under which it was registered at {@link TableRegistry}.
   */
  public String load() throws LoadException {
    Properties controlProperties;
    String fileName;
    String tableName;
    String type;
    long firstRowId;
    LoaderColumnInfo columnInfo;
    File file;
    Object sync = new Object();

    // We retry executing the following loading/validation of the control file itself. It could be that the load method
    // gets called too early, when the control file has not been fully written, therefore we'll retry.
    int maxRetries = 5;
    for (int retryNo = 0;; retryNo++) {
      try {
        controlProperties = new Properties();
        try (InputStream controlFileInputStream = new FileInputStream(controlFile)) {
          controlProperties.load(controlFileInputStream);
        } catch (IOException e) {
          throw new LoadException("Could not load information of control file " + controlFile.getAbsolutePath(), e);
        }
        fileName = controlProperties.getProperty(KEY_FILE);
        tableName = controlProperties.getProperty(KEY_TABLE);
        type = controlProperties.getProperty(KEY_TYPE);
        String firstRowIdString = controlProperties.getProperty(KEY_FIRST_ROWID);

        if (fileName == null || tableName == null || firstRowIdString == null || type == null
            || !(type.equals(TYPE_CSV) || type.equals(TYPE_JSON) || type.equals(TYPE_DIQUBE)))
          throw new LoadException("Invalid control file " + controlFile.getAbsolutePath());

        try {
          firstRowId = Long.parseLong(firstRowIdString);
        } catch (NumberFormatException e) {
          throw new LoadException(
              "Invalid control file " + controlFile.getAbsolutePath() + " (FirstRowId is no valid number)");
        }

        ColumnType defaultColumnType;
        String defaultColumnTypeString = controlProperties.getProperty(KEY_DEFAULT_COLTYPE);
        if (defaultColumnTypeString == null)
          defaultColumnType = ColumnType.STRING;
        else
          defaultColumnType = resolveColumnType(defaultColumnTypeString);

        columnInfo = new LoaderColumnInfo(defaultColumnType);

        for (Object key : controlProperties.keySet()) {
          String keyString = (String) key;
          if (keyString.startsWith(KEY_COLTYPE_PREFIX)) {
            String val = controlProperties.getProperty(keyString);
            keyString = keyString.substring(KEY_COLTYPE_PREFIX.length());
            // TODO #13 LoaderColumnInfo should be able to handle repeated columns nicely.
            columnInfo.registerColumnType(keyString, resolveColumnType(val));
          }
        }

        file = controlFile.toPath().resolveSibling(fileName).toFile();
        if (!file.exists() || !file.isFile())
          throw new LoadException("File " + file.getAbsolutePath() + " does not exist or is no file.");

        break;
      } catch (LoadException e) {
        if (retryNo == maxRetries - 1) {
          throw e;
        }

        logger.info("Was not able to load control file {}, will retry. Error: {}", controlFile.getAbsolutePath(),
            e.getMessage());
        synchronized (sync) {
          try {
            sync.wait(200);
          } catch (InterruptedException e1) {
            throw new LoadException("Interrupted while waiting to retry loading control file", e1);
          }
        }
      }
    }

    if (tableRegistry.getTable(tableName) != null)
      // TODO #33 support loading multiple shards of a table from multiple control files (and removing them
      // correctly).
      throw new LoadException("Table '" + tableName + "' already exists.");

    Loader loader;
    switch (type) {
    case TYPE_CSV:
      loader = csvLoader;
      break;
    case TYPE_JSON:
      loader = jsonLoader;
      break;
    case TYPE_DIQUBE:
      loader = diqubeLoader;
      break;
    default:
      throw new LoadException("Unkown input file type.");
    }
    TableShard newTableShard = loader.load(firstRowId, file.getAbsolutePath(), tableName, columnInfo);

    Collection<TableShard> newTableShardCollection = Arrays.asList(new TableShard[] { newTableShard });
    Table newTable = tableFactory.createTable(tableName, newTableShardCollection);

    tableRegistry.addTable(tableName, newTable);

    return tableName;

  }
}
