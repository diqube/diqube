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
package org.diqube.server.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.Function;

import org.diqube.context.Profiles;
import org.diqube.data.TableFactory;
import org.diqube.data.TableShard;
import org.diqube.execution.TableRegistry;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.server.ControlFileLoader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Table;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;

/**
 * Tests {@link ControlFileLoader}.
 *
 * @author Bastian Gloeckle
 */
public class ControlFileLoaderTest {

  /** Location in classpath where test files are available. */
  private static final String TESTDATA_CLASSPATH = "ControlFileLoaderTest/";
  /** A test .control-file with JSON & firstRowId = 0 */
  private static final String CONTROL_AGE_FIRSTROW0 = "age-firstrow0.control";
  /** A test .control-file with JSON & firstRowId = 5 */
  private static final String CONTROL_AGE_FIRSTROW5 = "age-firstrow5.control";
  private AnnotationConfigApplicationContext dataContext;

  /** Factory function for a {@link ControlFileLoader} based on a specific control file. */
  private Function<File, ControlFileLoader> controlFileFactory;
  /** All files in {@link #TESTDATA_CLASSPATH} are materialized in this directory for the tests. */
  private File testDir;
  /** The {@link TableRegistry} receiving the {@link Table} after it's been loaded by the tests. */
  private TableRegistry tableRegistry;

  @BeforeMethod
  public void setup() throws IOException {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    tableRegistry = dataContext.getBean(TableRegistry.class);

    controlFileFactory = new Function<File, ControlFileLoader>() {
      @Override
      public ControlFileLoader apply(File controlFile) {
        return new ControlFileLoader( //
            tableRegistry, //
            dataContext.getBean(TableFactory.class), //
            dataContext.getBean(CsvLoader.class), //
            dataContext.getBean(JsonLoader.class), //
            controlFile);
      }
    };

    testDir = File.createTempFile(ControlFileLoaderTest.class.getSimpleName(), Long.toString(System.nanoTime()));
    testDir.delete();
    testDir.mkdir();

    for (ResourceInfo resInfo : ClassPath.from(this.getClass().getClassLoader()).getResources()) {
      if (resInfo.getResourceName().startsWith(TESTDATA_CLASSPATH)) {
        String targetFileName = resInfo.getResourceName().substring(TESTDATA_CLASSPATH.length());
        try (FileOutputStream fos = new FileOutputStream(new File(testDir, targetFileName))) {
          InputStream is = this.getClass().getClassLoader().getResourceAsStream(resInfo.getResourceName());
          ByteStreams.copy(is, fos);
        }
      }
    }
  }

  @AfterMethod
  public void cleanup() throws IOException {
    // delete temp files
    Files.walkFileTree(testDir.toPath(), new FileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        file.toFile().delete();
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        dir.toFile().delete();
        return FileVisitResult.CONTINUE;
      }
    });
    dataContext.close();
  }

  @Test
  public void jsonFirstRow0() throws LoadException {
    // GIVEN
    ControlFileLoader cfl = controlFileFactory.apply(new File(testDir, CONTROL_AGE_FIRSTROW0));

    // WHEN
    String table = cfl.load();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertEquals(shard.getLowestRowId(), 0L, "Expected correct first row");
  }

  @Test
  public void jsonFirstRow5() throws LoadException {
    // GIVEN
    ControlFileLoader cfl = controlFileFactory.apply(new File(testDir, CONTROL_AGE_FIRSTROW5));

    // WHEN
    String table = cfl.load();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertEquals(shard.getLowestRowId(), 5L, "Expected correct first row");
  }
}
