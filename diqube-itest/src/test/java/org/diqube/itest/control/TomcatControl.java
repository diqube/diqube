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
package org.diqube.itest.control;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class capable of controlling a tomcat installation for the integration tests.
 *
 * @author Bastian Gloeckle
 */
public class TomcatControl implements LogfileSaver {
  private static final Logger logger = LoggerFactory.getLogger(TomcatControl.class);

  private boolean isStarted = false;
  private File tomcatDir;

  /**
   * @param unzipDirectory
   *          Directory where the tomcat has been extracted to.
   */
  public TomcatControl(File unzipDirectory) {
    String[] tomcatDirNames =
        unzipDirectory.list((dir, name) -> name.startsWith("apache-tomcat-") && new File(dir, name).isDirectory());
    if (tomcatDirNames.length != 1)
      throw new RuntimeException("Could not identify the directory containing the tomcat installation.");

    tomcatDir = new File(unzipDirectory, tomcatDirNames[0]);
    logger.info("Using tomcat installation directory '{}'", tomcatDir.getAbsolutePath());

    throw new RuntimeException("Cargo needed to be removed, therefore not implemented currently.");
  }

  /**
   * Start tomcat
   */
  public void start() {
    if (isStarted)
      throw new RuntimeException("Tomcat started already.");

    // TODO Bind to 127.0.0.1 only, find port dynamically.

    logger.info("Starting tomcat...");
    logger.info("Started tomcat.");
    isStarted = true;
  }

  /**
   * Stop tomcat
   */
  public void stop() {
    logger.info("Stopping tomcat...");
    isStarted = false;
    logger.info("Stopped tomcat.");
  }

  /**
   * @return true if tomcat is currently running.
   */
  public boolean isStarted() {
    return isStarted;
  }

  @Override
  public void saveLogfiles(File targetDir) {
    File logsDir = new File(tomcatDir, "logs");
    logger.info("Saving tomcat logfiles from '{}' to '{}'.", logsDir.getAbsolutePath(), targetDir.getAbsolutePath());
    Path targetPath = targetDir.toPath();
    for (File logFile : logsDir.listFiles()) {
      try {
        Files.copy(logFile.toPath(), targetPath.resolve(logFile.getName()));
      } catch (IOException e) {
        logger.warn("Could not save logfile {}", logFile);
      }
    }
  }
}
