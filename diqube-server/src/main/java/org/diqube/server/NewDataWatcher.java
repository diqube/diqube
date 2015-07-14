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
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.data.TableFactory;
import org.diqube.execution.TableRegistry;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.server.config.Config;
import org.diqube.server.config.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;

/**
 * Watches a directory for new data that should be loaded into the currently running diqube server (checks for .control
 * files).
 * 
 * <p>
 * This bean will only be auto instantiated, if {@link Profiles#NEW_DATA_WATCHER} is enabled.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.NEW_DATA_WATCHER)
public class NewDataWatcher {

  private static final Logger logger = LoggerFactory.getLogger(NewDataWatcher.class);

  private static final String CONTROL_FILE_EXTENSION = ".control";

  @Config(ConfigKey.DATA_DIR)
  private String directory;

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private TableFactory tableFactory;

  @Inject
  private CsvLoader csvLoader;

  @Inject
  private JsonLoader jsonLoader;

  private WatchService watchService;

  private NewDataWatchThread thread;

  private Path watchPath;

  private Map<String, String> tableNamesByControlFilePath = new HashMap<>();

  @PostConstruct
  public void initialize() {
    watchPath = Paths.get(directory).toAbsolutePath();
    File f = watchPath.toFile();
    if (!f.exists() || !f.isDirectory()) {
      logger.error("{} is no valid directory.", watchPath);
      throw new RuntimeException(watchPath + " is no valid directory.");
    }

    List<File> initialControlFiles = Arrays.asList(watchPath.toFile().listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(CONTROL_FILE_EXTENSION);
      }
    }));

    try {
      watchService = watchPath.getFileSystem().newWatchService();
      watchPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
      logger.info("Will watch {} for new data.", watchPath);
    } catch (IOException e) {
      logger.error("Could not instantiate WatchService and register events", e);
      throw new RuntimeException("Could not instantiate WatchService and register events", e);
    }

    thread = new NewDataWatchThread(initialControlFiles);
    thread.start();
  }

  @PreDestroy
  public void destruct() {
    thread.interrupt();
  }

  private void loadControlFile(File controlFile) {
    logger.info("Found new control file {}. Starting to load new table shard.", controlFile.getAbsolutePath());
    try {
      String tableName = new ControlFileLoader(tableRegistry, tableFactory, csvLoader, jsonLoader, controlFile).load();
      tableNamesByControlFilePath.put(controlFile.getAbsolutePath(), tableName);
      logger.info("New table {} loaded successfully from {}'", tableName, controlFile.getAbsolutePath());
    } catch (LoadException e) {
      logger.error("Could not load new table shard from {}", controlFile.getAbsolutePath(), e);
    }
  }

  private void unloadControlFile(File controlFile) {
    String tableName = tableNamesByControlFilePath.get(controlFile.getAbsolutePath());
    // TODO #33 support loading multiple shards.
    if (tableName == null) {
      logger.warn("Identified deletion of control file {}, but could not resolve the table that was loaded from "
          + "that file. Will not remove any in-memory data.", controlFile.getAbsolutePath());
      return;
    }

    logger.info("Identified deletion of control file {}; will remove in-memory data for table {}.",
        controlFile.getAbsolutePath(), tableName);
    tableRegistry.removeTable(tableName);
    tableNamesByControlFilePath.remove(controlFile.getAbsolutePath());
    System.gc();
  }

  /**
   * Thread that keeps polling the {@link WatchService} for updates.
   */
  private class NewDataWatchThread extends Thread {

    private List<File> initialControlFiles;

    public NewDataWatchThread(List<File> initialControlFiles) {
      super(NewDataWatcher.class.getSimpleName());
      this.initialControlFiles = initialControlFiles;
    }

    @Override
    public void run() {
      for (File controlFile : initialControlFiles)
        loadControlFile(controlFile);

      while (true) {
        WatchKey watchKey;
        try {
          watchKey = watchService.take();
        } catch (InterruptedException e) {
          logger.error("Interrupted while watching directory {} for changes. Monitoring stops.", watchPath);
          return;
        }

        for (WatchEvent<?> event : watchKey.pollEvents()) {
          Path createdPath = ((Path) watchKey.watchable()).resolve((Path) event.context());
          File createdFile = createdPath.toFile();
          if (event.kind().equals(StandardWatchEventKinds.ENTRY_CREATE)) {
            if (createdFile.isFile() && createdFile.getName().toLowerCase().endsWith(CONTROL_FILE_EXTENSION))
              loadControlFile(createdFile);
          } else if (event.kind().equals(StandardWatchEventKinds.ENTRY_DELETE)) {
            if (createdFile.getName().toLowerCase().endsWith(CONTROL_FILE_EXTENSION))
              unloadControlFile(createdFile);
          }
        }

        if (!watchKey.reset()) {
          logger.warn("The WatchKey on the {} ({}) was unregistered. Will not monitor any directory for new "
              + "data any more.", ConfigKey.DATA_DIR, watchPath);
          return;
        }
      }
    }
  }
}
