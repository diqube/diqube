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
package org.diqube.server.control;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.thrift.TException;
import org.diqube.data.column.ColumnType;
import org.diqube.data.table.AdjustableTable;
import org.diqube.data.table.AdjustableTable.TableShardsOverlappingException;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.TableRegistry;
import org.diqube.executionenv.TableRegistry.TableLoadImpossibleException;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.loader.Loader;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.server.metadata.ServerTableMetadataPublisher;
import org.diqube.server.metadata.ServerTableMetadataPublisher.MergeImpossibleException;
import org.diqube.server.queryremote.flatten.ClusterFlattenServiceHandler;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.thrift.base.util.RUuidUtil;
import org.diqube.util.Pair;
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
  public static final String KEY_AUTO_FLATTEN = "autoFlatten";

  public static final String TYPE_CSV = "csv";
  public static final String TYPE_JSON = "json";
  public static final String TYPE_DIQUBE = "diqube";

  private File controlFile;
  private TableRegistry tableRegistry;
  private TableFactory tableFactory;
  private CsvLoader csvLoader;
  private JsonLoader jsonLoader;
  private DiqubeLoader diqubeLoader;
  private ClusterFlattenServiceHandler clusterFlattenServiceHandler;
  private ServerTableMetadataPublisher metadataPublisher;

  private Object tableRegistrySync = new Object();

  public ControlFileLoader(TableRegistry tableRegistry, TableFactory tableFactory, CsvLoader csvLoader,
      JsonLoader jsonLoader, DiqubeLoader diqubeLoader, ClusterFlattenServiceHandler clusterFlattenServiceHandler,
      ServerTableMetadataPublisher metadataPublisher, File controlFile) {
    this.tableRegistry = tableRegistry;
    this.tableFactory = tableFactory;
    this.csvLoader = csvLoader;
    this.jsonLoader = jsonLoader;
    this.diqubeLoader = diqubeLoader;
    this.clusterFlattenServiceHandler = clusterFlattenServiceHandler;
    this.metadataPublisher = metadataPublisher;
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
   * <p>
   * This method takes care of starting a flattening on the table if "autoFlatten" is available in control file.
   * 
   * <p>
   * This method takes care of calculating {@link TableMetadata} for the resulting table and publish this information in
   * the cluster.
   *
   * <p>
   * Note that .ready files will not be created.
   * 
   * @return The name of the table under which it was registered at {@link TableRegistry} and a List containing the
   *         values of {@link TableShard#getLowestRowId()} of the table shard(s) that were loaded.
   */
  public Pair<String, List<Long>> load() throws LoadException {
    Properties controlProperties;
    String fileName;
    String tableName;
    String type;
    String[] autoFlatten;
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

        String autoFlattenUnsplit = controlProperties.getProperty(KEY_AUTO_FLATTEN, "");
        if (!"".equals(autoFlattenUnsplit)) {
          autoFlatten = autoFlattenUnsplit.split(",");
          for (int i = 0; i < autoFlatten.length; i++)
            autoFlatten[i] = autoFlatten[i].trim();
        } else
          autoFlatten = new String[0];

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

    Collection<TableShard> newTableShards = loader.load(firstRowId, file.getAbsolutePath(), tableName, columnInfo);
    synchronized (tableRegistrySync) {
      Table table = tableRegistry.getTable(tableName);
      if (table != null) {
        if (!(table instanceof AdjustableTable))
          throw new LoadException("The target table '" + tableName + "' cannot be adjusted.");

        List<TableShard> allShards = new ArrayList<>(table.getShards());
        allShards.addAll(newTableShards);

        distributeNewMetadata(tableName, allShards);

        try {
          for (TableShard newTableShard : newTableShards)
            ((AdjustableTable) table).addTableShard(newTableShard);
        } catch (TableShardsOverlappingException e) {

          // remove all those shards that might've been added already.
          for (TableShard newTableShard : newTableShards)
            ((AdjustableTable) table).removeTableShard(newTableShard);

          throw new LoadException("Cannot load TableShard as it overlaps with an already loaded one", e);
        }
      } else {
        Collection<TableShard> newTableShardCollection = newTableShards;
        table = tableFactory.createDefaultTable(tableName, newTableShardCollection);

        distributeNewMetadata(tableName, newTableShardCollection);

        try {
          tableRegistry.addTable(tableName, table);
        } catch (TableLoadImpossibleException e) {
          throw new LoadException("Cannot load table " + table, e);
        }
      }
    }

    // For "auto-flattening" we use the clusterFlattenServiceHandler directly with an empty list of "other flatteners"
    // and a null-resultAddress. Using this, this node will merge new flatten requests on that table to the one we start
    // now, although these might fail (as other requests probably include multiple flatteners; our node will though only
    // flatten on ourselves; but query masters that issued the flattening should be able to cope with that).
    for (String autoFlattenField : autoFlatten) {
      UUID flattenId = UUID.randomUUID();
      try {
        // these calls start the flattening asynchronously, therefore we just trigger computation here. If there is a
        // flattened version available in the flattenedDiskCache already, that will be used.
        clusterFlattenServiceHandler.flattenAllLocalShards(RUuidUtil.toRUuid(flattenId), tableName, autoFlattenField,
            new ArrayList<>(), null);
      } catch (TException e) {
        logger.error("Failed to flatten new table '{}' by '{}' locally with flatten ID {}.", tableName,
            autoFlattenField, flattenId, e);
      }
    }

    List<Long> firstRowIds =
        newTableShards.stream().map(shard -> shard.getLowestRowId()).sorted().collect(Collectors.toList());
    return new Pair<>(tableName, firstRowIds);
  }

  /**
   * Creates new {@link TableMetadata} for the given table with the given shards (all shards of table, with the new
   * ones) and distributes it across the cluster. Throws {@link LoadException} if metadata is incompatible in any way
   * and no new shards should be loaded at all now.
   */
  private void distributeNewMetadata(String tableName, Collection<TableShard> allShards) throws LoadException {
    try {
      metadataPublisher.publishMetadataOfTableShards(tableName, allShards);
    } catch (MergeImpossibleException e) {
      throw new LoadException("Cannot load table '" + tableName + "' since its metadata is incompatible", e);
    }
  }
}
