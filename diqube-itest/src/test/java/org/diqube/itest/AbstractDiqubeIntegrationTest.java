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
package org.diqube.itest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.annotations.NeedsTomcat;
import org.diqube.itest.control.LogfileSaver;
import org.diqube.itest.control.ServerClusterControl;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.control.TomcatControl;
import org.diqube.itest.control.ToolControl;
import org.diqube.itest.util.Unzip;
import org.diqube.itest.util.Zip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.io.ByteStreams;

/**
 * Abstract base class for all integration tests.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(AbstractDiqubeIntegrationTest.class);

  /** (required) local location of the jar of diqube-server */
  private static final String PROP_SERVER_JAR = "diqube.itest.server.jar";
  /** (required) local location of the jar of diqube-tool */
  private static final String PROP_TOOL_JAR = "diqube.itest.tool.jar";
  /** (required) local location of the war of diqube-ui */
  private static final String PROP_UI_WAR = "diqube.itest.ui.war";
  /** (required) local location of the zip containing a tomcat */
  private static final String PROP_TOMCAT_ZIP = "diqube.itest.tomcat.zip";
  /** (required) local directory where the tests can write files to */
  private static final String PROP_WORK_DIR = "diqube.itest.work.dir";
  /**
   * (optional) delete the {@link #testWorkDir}s of all the test methods. If set, set {@link #PROP_TARGET_LOG_DIR}, too!
   */
  private static final String PROP_DELTE_TEST_DIRS = "diqube.itest.delete.test.work.dirs";
  /**
   * (optional) before deleting the {@link #testWorkDir}s of test methods, save the logfiles. Collect all the logfiles
   * in this directory (these will be .zip files in child directories).
   */
  private static final String PROP_TARGET_LOG_DIR = "diqube.itest.target.log.dir";
  /**
   * System property name which needs the number of the server to be attached. If it is set, {@link ServerControl} will
   * not try to start a separate diqube-server, but expects one to be running. Please note that that server needs to
   * have to correct properties set (like dataDir, port etc.).
   * 
   * This can be used to debug.
   */
  public static final String PROP_SERVER_OVERRIDE = "diqube.itest.server.override.";

  private File serverJarFile;
  private File toolJarFile;
  private File uiWarFile;
  private File tomcatZipFile;
  private File classWorkDir;
  private File targetLogDir;
  private boolean deleteTestWorkDirs;
  /** A directory local to the test emthod where it can place some files etc. */
  protected File testWorkDir = null;
  /**
   * The {@link TomcatControl} of a tomcat instance for the test method (only available if test method is annotated with
   * {@link NeedsTomcat}).
   */
  protected TomcatControl tomcatControl = null;
  /** {@link ToolControl} for each test method. */
  protected ToolControl toolControl;
  /**
   * contains the serverControls for the diqube-servers. Only available when test method is annotated with
   * {@link NeedsServer}. See also {@link #clusterControl}.
   */
  protected List<ServerControl> serverControl = null;
  /** Controls a whole cluster of diqube-servers - combines the {@link #serverControl}s. */
  protected ServerClusterControl clusterControl;

  public AbstractDiqubeIntegrationTest() {
    String serverJarFileName = System.getProperty(PROP_SERVER_JAR);
    String toolJarFileName = System.getProperty(PROP_TOOL_JAR);
    String uiWarFileName = System.getProperty(PROP_UI_WAR);
    String tomcatZipFileName = System.getProperty(PROP_TOMCAT_ZIP);
    String workDirName = System.getProperty(PROP_WORK_DIR);
    String targetLogDirName = System.getProperty(PROP_TARGET_LOG_DIR);
    deleteTestWorkDirs = System.getProperty(PROP_DELTE_TEST_DIRS) != null;

    if (serverJarFileName == null || toolJarFileName == null || uiWarFileName == null || tomcatZipFileName == null
        || workDirName == null)
      throw new RuntimeException("Not all system properties available.");

    serverJarFile = new File(serverJarFileName);
    toolJarFile = new File(toolJarFileName);
    uiWarFile = new File(uiWarFileName);
    tomcatZipFile = new File(tomcatZipFileName);
    targetLogDir = (targetLogDirName != null) ? new File(targetLogDirName) : null;
    File rootWorkDir = new File(workDirName);

    if (!serverJarFile.exists() || !serverJarFile.isFile() || !toolJarFile.exists() || !toolJarFile.isFile()
        || !uiWarFile.exists() || !uiWarFile.isFile() || !tomcatZipFile.exists() || !tomcatZipFile.isFile())
      throw new RuntimeException("Not all input files present.");

    classWorkDir = new File(rootWorkDir, this.getClass().getSimpleName());

    if (classWorkDir.exists())
      deleteDirRecursively(classWorkDir.toPath());

    ensureDirExists(classWorkDir);
  }

  @BeforeMethod
  public void initializeTestMethodWorkDirectory(Method testMethod) {
    testWorkDir = new File(classWorkDir, testMethod.getName());
    ensureDirExists(testWorkDir);

    if (testMethod.isAnnotationPresent(NeedsTomcat.class)) {
      new Unzip(tomcatZipFile).unzip(testWorkDir);
      tomcatControl = new TomcatControl(testWorkDir);
    } else
      tomcatControl = null;

    if (testMethod.isAnnotationPresent(NeedsServer.class)) {
      serverControl = new ArrayList<>();
      NeedsServer annotation = testMethod.getAnnotation(NeedsServer.class);
      clusterControl = new ServerClusterControl();
      for (int i = 0; i < annotation.servers(); i++) {
        boolean manualOverride = System.getProperty(PROP_SERVER_OVERRIDE + i) != null;

        if (manualOverride) {
          try {
            // We might just have deleted the workDir (from a previous run, workDirs are deleted in constructor). Give
            // the manual diqube-server here some time to recognize that the work dir was deleted (in order that it will
            // be able to re-attach the data dir to the new dir!)
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        File serverWorkDir = new File(testWorkDir, "server-" + i);
        ensureDirExists(serverWorkDir);
        serverControl
            .add(new ServerControl(serverJarFile, serverWorkDir, clusterControl, clusterControl, manualOverride));
      }
      clusterControl.setServers(serverControl);

      if (!annotation.manualStart())
        clusterControl.start();
    } else {
      serverControl = null;
      clusterControl = null;
    }

    toolControl = new ToolControl(toolJarFile);
  }

  @AfterMethod
  public void cleanupTestMethodWorkDirectory(Method testMethod) {
    if (tomcatControl != null && tomcatControl.isStarted())
      tomcatControl.stop();

    if (clusterControl != null)
      clusterControl.stop();

    if (targetLogDir != null) {
      File logResultDir = new File(testWorkDir, "result_logs");
      ensureDirExists(logResultDir);

      if (tomcatControl != null)
        saveLogs(tomcatControl, logResultDir, "tomcat");
      for (int i = 0; i < serverControl.size(); i++)
        saveLogs(serverControl.get(i), logResultDir, "server-" + i);

      ensureDirExists(targetLogDir);
      Zip zip = new Zip();
      File zipFile = new File(targetLogDir, this.getClass().getSimpleName() + "_" + testMethod.getName() + ".zip");
      zip.zip(logResultDir, zipFile);
      logger.info("Collected logs and zipped them to '{}'", zipFile.getAbsolutePath());
    }

    if (deleteTestWorkDirs) {
      logger.info("Removing work directory of the test.");
      deleteDirRecursively(testWorkDir.toPath());
    }
    testWorkDir = null;
    tomcatControl = null;
    toolControl = null;
    serverControl = null;
    clusterControl = null;
  }

  private void saveLogs(LogfileSaver logfileSaver, File logBaseDir, String subdir) {
    File logResultDir = new File(logBaseDir, subdir);
    ensureDirExists(logResultDir);
    logfileSaver.saveLogfiles(logResultDir);
  }

  private void ensureDirExists(File dir) {
    if (!dir.exists())
      if (!dir.mkdirs())
        throw new RuntimeException("Could not create directory " + dir.getAbsolutePath());
  }

  protected void deleteDirRecursively(Path dir) {
    try {
      Files.walkFileTree(dir, new FileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException("Could not clean work directory", e);
    }
  }

  /**
   * Fetches a file from the classpath, writes it to {@link #testWorkDir} and returns a {@link File} pointing to it.
   * 
   * @param fileName
   *          Name of the file on the classpath.
   */
  protected File cp(String fileName) {
    InputStream is = this.getClass().getResourceAsStream(fileName);

    if (is == null)
      throw new RuntimeException("File not on classpath: " + fileName);

    if (fileName.startsWith("/"))
      fileName = fileName.substring(1);

    File targetFile = new File(new File(testWorkDir, "res"), fileName);
    ensureDirExists(targetFile.getParentFile());
    try (FileOutputStream fos = new FileOutputStream(targetFile)) {
      ByteStreams.copy(is, fos);
    } catch (IOException e) {
      throw new RuntimeException("Could not write classpath file to HDD: " + fileName, e);
    }

    logger.info("Wrote '{}' to '{}'.", fileName, targetFile.getAbsolutePath());

    return targetFile;
  }

  /**
   * Resolves a filename inside {@link #testWorkDir}
   * 
   * @param fileName
   *          Name of the file relative to {@link #testWorkDir}.
   */
  protected File work(String fileName) {
    if (fileName.startsWith("/"))
      fileName = fileName.substring(1);

    return new File(testWorkDir, fileName);
  }
}
