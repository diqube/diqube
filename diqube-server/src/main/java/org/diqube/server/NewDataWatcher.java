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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.ConfigKey;
import org.diqube.config.DerivedConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.listeners.ConsensusListener;
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
 * <p>
 * The implementation is somewhat tightly connected to {@link ControlFileManager}.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.NEW_DATA_WATCHER)
public class NewDataWatcher implements ConsensusListener {

  private static final Logger logger = LoggerFactory.getLogger(NewDataWatcher.class);

  @Config(DerivedConfigKey.FINAL_DATA_DIR)
  private String directory;

  @Inject
  private ControlFileManager controlFileManager;

  private NewDataWatchThread thread;

  private Path watchPath;

  @Override
  public void consensusInitialized() {
    // Start initializing as soon as we're ready to communicate with the cluster.
    watchPath = Paths.get(directory).toAbsolutePath();
    File f = watchPath.toFile();
    if (!f.exists() || !f.isDirectory()) {
      logger.error("{} is no valid directory.", watchPath);
      throw new RuntimeException(watchPath + " is no valid directory.");
    }

    // delete all initial ready/failure files.
    List<File> readyFiles = Arrays.asList(watchPath.toFile()
        .listFiles((dir, fileName) -> fileName.toLowerCase().endsWith(ControlFileManager.READY_FILE_EXTENSION)
            || fileName.toLowerCase().endsWith(ControlFileManager.FAILURE_FILE_EXTENSION)));
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

  private void deployControlFile(File controlFile) {
    logger.info("Found new control file {}.", controlFile.getAbsolutePath());
    controlFileManager.deployControlFile(controlFile);
  }

  private void undeployControlFile(File controlFile) {
    logger.info("Control file was deleted: {}", controlFile.getAbsolutePath());
    controlFileManager.undeployControlFile(controlFile);
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

          File[] controlFiles = watchPath.toFile()
              .listFiles((dir, fileName) -> fileName.toLowerCase().endsWith(ControlFileManager.CONTROL_FILE_EXTENSION));

          // controlFiles is null if watchPath does not exist.
          if (controlFiles != null) {
            for (File controlFile : controlFiles)
              deployControlFile(controlFile);
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
              if (createdFile.isFile()
                  && createdFile.getName().toLowerCase().endsWith(ControlFileManager.CONTROL_FILE_EXTENSION))
                deployControlFile(createdFile);
            } else if (event.kind().equals(StandardWatchEventKinds.ENTRY_DELETE)) {
              if (createdFile.getName().toLowerCase().endsWith(ControlFileManager.CONTROL_FILE_EXTENSION))
                undeployControlFile(createdFile);
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
