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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.data.Table;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.execution.TableRegistry;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;

/**
 * Watches a directory for new data that should be loaded into the currently running diqube server (checks for .control
 * files).
 * 
 * <p>
 * After the data of a control file has been loaded, a .ready file will be placed right next to it. The .ready file will
 * be removed when unloading the control file.
 * 
 * <p>
 * This bean will only be auto instantiated, if {@link Profiles#NEW_DATA_WATCHER} is enabled.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.NEW_DATA_WATCHER)
public class NewDataWatcher implements ClusterManagerListener {

  private static final Logger logger = LoggerFactory.getLogger(NewDataWatcher.class);

  public static final String CONTROL_FILE_EXTENSION = ".control";
  public static final String READY_FILE_EXTENSION = ".ready";

  @Config(ConfigKey.DATA_DIR)
  private String directory;

  @Inject
  private TableRegistry tableRegistry;

  @Inject
  private TableFactory tableFactory;

  @Inject
  private CsvLoader csvLoader;

  @Inject
  private DiqubeLoader diqubeLoader;

  @Inject
  private JsonLoader jsonLoader;

  private NewDataWatchThread thread;

  private Path watchPath;

  /**
   * Map from controlFile path to a pair of table name and a list of values of {@link TableShard#getLowestRowId()} of
   * the tableShards that were loaded from that file.
   */
  private Map<String, Pair<String, List<Long>>> tableInfoByControlFilePath = new HashMap<>();

  @Override
  public void clusterInitialized() {
    // Start initializing as soon as we're ready to communicate with the cluster.
    watchPath = Paths.get(directory).toAbsolutePath();
    File f = watchPath.toFile();
    if (!f.exists() || !f.isDirectory()) {
      logger.error("{} is no valid directory.", watchPath);
      throw new RuntimeException(watchPath + " is no valid directory.");
    }

    // delete all initial ready files.
    List<File> readyFiles = Arrays
        .asList(watchPath.toFile().listFiles((dir, fileName) -> fileName.toLowerCase().endsWith(READY_FILE_EXTENSION)));
    for (File statusFile : readyFiles)
      statusFile.delete();

    thread = new NewDataWatchThread(() -> {
      try {
        WatchService watchService = watchPath.getFileSystem().newWatchService();
        watchPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
        logger.info("Will watch {} for new data.", watchPath);
        return watchService;
      } catch (Exception e) {
        logger.warn("Could not install WatchService to watch dataDir {}. Will retry later.", watchPath, e);
        return null;
      }
    });
    thread.start();
  }

  @PreDestroy
  public void destruct() {
    thread.interrupt();
  }

  private void loadControlFile(File controlFile) {
    if (tableInfoByControlFilePath.containsKey(controlFile.getAbsolutePath())) {
      logger.info("Found control file {}, but that is loaded already. Skipping.", controlFile.getAbsolutePath());
      return;
    }

    logger.info("Found new control file {}. Starting to load new table shard.", controlFile.getAbsolutePath());
    try {
      Pair<String, List<Long>> tableInfo =
          new ControlFileLoader(tableRegistry, tableFactory, csvLoader, jsonLoader, diqubeLoader, controlFile).load();
      tableInfoByControlFilePath.put(controlFile.getAbsolutePath(), tableInfo);
      logger.info("Data for table '{}' (with starting rowIds {}) loaded successfully from {}'", tableInfo.getLeft(),
          tableInfo.getRight(), controlFile.getAbsolutePath());

      // write ready file
      String content = LocalDateTime.now().toString();
      try (FileOutputStream readyOS = new FileOutputStream(readyFile(controlFile))) {
        readyOS.write(content.getBytes(Charset.forName("UTF-8")));
      } catch (IOException e) {
        logger.warn("Could not write ready file {}", readyFile(controlFile), e);
      }
    } catch (LoadException e) {
      logger.error("Could not load new table shard from {}", controlFile.getAbsolutePath(), e);
    }
  }

  private void unloadControlFile(File controlFile) {
    Pair<String, List<Long>> tableInfo = tableInfoByControlFilePath.get(controlFile.getAbsolutePath());
    if (tableInfo == null) {
      logger.warn("Identified deletion of control file {}, but could not resolve the table that data from that file "
          + "was loaded to. Will not remove any in-memory data.", controlFile.getAbsolutePath());
      return;
    }

    File readyFile = readyFile(controlFile);
    if (readyFile.exists())
      if (!readyFile.delete())
        logger.warn("Could not delete ready file {}", readyFile.getAbsolutePath());

    Table t = tableRegistry.getTable(tableInfo.getLeft());
    if (t == null)
      logger.warn("Could not delete anything as table {} is not loaded (anymore?).", tableInfo.getLeft());
    else {
      logger.info(
          "Identified deletion of control file {}; will remove in-memory data from table {} for TableShards with starting rowIds {}.",
          controlFile.getAbsolutePath(), tableInfo.getLeft(), tableInfo.getRight());
      List<TableShard> shardsToDelete = t.getShards().stream()
          .filter(s -> tableInfo.getRight().contains(s.getLowestRowId())).collect(Collectors.toList());
      shardsToDelete.forEach(s -> t.removeTableShard(s));
      if (t.getShards().isEmpty()) {
        logger.info("Removed last table shard of table '{}', will stop serving this table completely.",
            tableInfo.getLeft());
        tableRegistry.removeTable(tableInfo.getLeft());
      }
      tableInfoByControlFilePath.remove(controlFile.getAbsolutePath());
      System.gc();
    }
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
   * Thread that keeps polling the {@link WatchService} for updates.
   */
  private class NewDataWatchThread extends Thread {

    private Supplier<WatchService> watchServiceSupplier;

    public NewDataWatchThread(Supplier<WatchService> watchServiceRegistrationFn) {
      super(NewDataWatcher.class.getSimpleName());
      this.watchServiceSupplier = watchServiceRegistrationFn;
      this.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error("Uncaught exception in NewDataWatchThread. Will stop watching the directory. "
              + "Fix the issue and restart the server.", e);
        }
      });
    }

    @Override
    public void run() {
      WatchService watchService = null;
      Object sync = new Object();

      while (true) {
        if (watchService == null) {
          watchService = watchServiceSupplier.get();

          File[] controlFiles =
              watchPath.toFile().listFiles((dir, fileName) -> fileName.toLowerCase().endsWith(CONTROL_FILE_EXTENSION));

          // controlFiles is null if watchPath does not exist.
          if (controlFiles != null) {
            for (File controlFile : controlFiles)
              loadControlFile(controlFile);
          }

          if (watchService == null) {
            // still no watchService.
            synchronized (sync) {
              try {
                sync.wait(500);
              } catch (InterruptedException e) {
                // interrupted, die quietly.
                return;
              }
            }
            continue;
          }
        }

        WatchKey watchKey;
        try {
          watchKey = watchService.poll(500, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          logger.error("Interrupted while watching directory {} for changes. Monitoring stops.", watchPath);
          return;
        }

        if (watchKey != null) {
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
        }

        if ((watchKey != null && !watchKey.reset()) || !watchPath.toFile().exists()) {
          logger.warn("The WatchKey on the {} ({}) was unregistered/the directory was deleted.", ConfigKey.DATA_DIR,
              watchPath);
          try {
            watchService.close();
          } catch (IOException e) {
            // swallow.
          }
          watchService = null;
        }
      }
    }
  }

}
