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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.data.table.TableShard;
import org.diqube.loader.LoadException;
import org.diqube.server.control.ControlFileFactory;
import org.diqube.server.control.ControlFileUnloader;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the .control files that are provided to diqube at runtime and maintains corresponding .read and .failure
 * files.
 *
 * <p>
 * After the data of a control file has been loaded, a .ready file will be placed right next to it. The .ready file will
 * be removed when unloading the control file.
 * 
 * <p>
 * Additionally, during operation of the server, a .failure file may be written by this class if an error was
 * encountered with this servers data of the table. In that case, the data will be undeployed automatically and the
 * failure file will contain details of the problem.
 * 
 * <p>
 * The implementation is somewhat tightly connected to {@link NewDataWatcher}.
 * 
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ControlFileManager {
  private static final Logger logger = LoggerFactory.getLogger(ControlFileManager.class);

  public static final String CONTROL_FILE_EXTENSION = ".control";
  public static final String READY_FILE_EXTENSION = ".ready";
  public static final String FAILURE_FILE_EXTENSION = ".failure";

  @Inject
  private ControlFileFactory controlFileFactory;

  /**
   * Map from controlFile path to a pair of table name and a list of values of {@link TableShard#getLowestRowId()} of
   * the tableShards that were loaded from that file.
   */
  private Map<String, Pair<String, List<Long>>> tableInfoByControlFilePath = new HashMap<>();

  /**
   * Fully deploy a specific control file (if possible) and maintain .ready file.
   */
  public synchronized void deployControlFile(File controlFile) {
    if (tableInfoByControlFilePath.containsKey(controlFile.getAbsolutePath())) {
      logger.info("Control file {} is loaded already. Skipping.", controlFile.getAbsolutePath());
      return;
    }

    logger.info("Starting to load new table shard from control file {}.", controlFile.getAbsolutePath());
    try {
      Pair<String, List<Long>> tableInfo = controlFileFactory.createControlFileLoader(controlFile).load();
      tableInfoByControlFilePath.put(controlFile.getAbsolutePath(), tableInfo);
      logger.info("Data for table '{}' (with starting rowIds {}) loaded successfully from {}'", tableInfo.getLeft(),
          tableInfo.getRight(), controlFile.getAbsolutePath());

      File failure = failureFile(controlFile);
      if (failure.exists())
        if (!failure.delete())
          logger.warn("Could not delete failure file {}", failureFile(controlFile));

      // write ready file
      String content = LocalDateTime.now().toString();
      try (FileOutputStream readyOS = new FileOutputStream(readyFile(controlFile))) {
        readyOS.write(content.getBytes(Charset.forName("UTF-8")));
      } catch (IOException e) {
        logger.warn("Could not write ready file {}", readyFile(controlFile), e);
      }
    } catch (LoadException e) {
      logger.error("Could not load table shards from {}", controlFile.getAbsolutePath(), e);
    }
  }

  /**
   * Fully undeploy a specific control file and remove .ready file. This method is for a regular undeployment.
   */
  public synchronized void undeployControlFile(File controlFile) {
    if (internalUndeployControlFile(controlFile, true)) {
      File readyFile = readyFile(controlFile);
      if (readyFile.exists())
        if (!readyFile.delete())
          logger.warn("Could not delete ready file {}", readyFile.getAbsolutePath());

      File failure = failureFile(controlFile);
      if (failure.exists())
        if (!failure.delete())
          logger.warn("Could not delete failure file {}", failureFile(controlFile));
    }
  }

  /**
   * Fully undeploy a specific control file and remove .ready file.
   * 
   * @param handleMetadataChange
   *          see {@link ControlFileUnloader#unload(boolean)}
   */
  private boolean internalUndeployControlFile(File controlFile, boolean handleMetadataChange) {
    Pair<String, List<Long>> tableInfo = tableInfoByControlFilePath.get(controlFile.getAbsolutePath());

    if (tableInfo == null) {
      logger.warn(
          "Could not resolve the table that data of control file {} was loaded to. Will not remove any in-memory data.",
          controlFile.getAbsolutePath());
      return false;
    }

    controlFileFactory.createControlFileUnloader(controlFile, tableInfo).unload(handleMetadataChange);

    tableInfoByControlFilePath.remove(controlFile.getAbsolutePath());

    return true;
  }

  /**
   * Undeploy all control files of a specific table because there was an error when processing the table - a .failure
   * file will be written.
   * 
   * @param tableName
   *          The table of which to undeploy all control files
   * @param errorDescription
   *          The description of the error, will be written to the .failure file.
   * @param handleMetadataChange
   *          see {@link ControlFileUnloader#unload(boolean)}
   */
  public synchronized void undeployTableBecauseOfError(String tableName, String errorDescription,
      boolean handleMetadataChange) {
    List<File> controlFiles = new ArrayList<>();
    for (Entry<String, Pair<String, List<Long>>> entry : tableInfoByControlFilePath.entrySet()) {
      if (entry.getValue().getLeft().equals(tableName)) {
        controlFiles.add(new File(entry.getKey()));
      }
    }

    for (File controlFile : controlFiles) {
      logger.info("Undeploying control file {} because of an error, writing {} file.", controlFile,
          FAILURE_FILE_EXTENSION);

      internalUndeployControlFile(controlFile, handleMetadataChange);

      File readyFile = readyFile(controlFile);
      if (readyFile.exists())
        if (!readyFile.delete())
          logger.warn("Could not delete ready file {}", readyFile.getAbsolutePath());

      File failureFile = failureFile(controlFile);
      if (!failureFile.exists()) {
        try (FileOutputStream failureOS = new FileOutputStream(failureFile)) {
          failureOS.write(errorDescription.getBytes(Charset.forName("UTF-8")));
        } catch (IOException e) {
          logger.warn("Could not write failure file {}", failureFile, e);
        }
      }
    }
  }

  /**
   * See {@link #undeployTableBecauseOfError(String, String)}, errorDescription is taken from given Throwable.
   */
  public synchronized void undeployTableBecauseOfError(String tableName, Throwable t, boolean handleMetadataChange) {
    undeployTableBecauseOfError(tableName, t.getMessage(), handleMetadataChange);
  }

  /**
   * @return The ready file for a given control file.
   */
  private File readyFile(File controlFile) {
    return new File(controlFile.getParentFile(),
        controlFile.getName().substring(0, controlFile.getName().length() - CONTROL_FILE_EXTENSION.length())
            + READY_FILE_EXTENSION);
  }

  /**
   * @return The failure file for a given control file.
   */
  private File failureFile(File controlFile) {
    return new File(controlFile.getParentFile(),
        controlFile.getName().substring(0, controlFile.getName().length() - CONTROL_FILE_EXTENSION.length())
            + FAILURE_FILE_EXTENSION);
  }
}
