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

import org.diqube.consensus.ConsensusServer;
import org.diqube.consensus.ConsensusServerTestUtil;
import org.diqube.context.Profiles;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.TableRegistry;
import org.diqube.listeners.ClusterManagerListener;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.server.ControlFileLoader;
import org.diqube.server.NewDataWatcher;
import org.diqube.server.queryremote.flatten.ClusterFlattenServiceHandler;
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
  private static final String CONTROL_AGE_FIRSTROW0 = "age-firstrow0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  /** A test .control-file with JSON & firstRowId = 5 */
  private static final String CONTROL_AGE_FIRSTROW5 = "age-firstrow5" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "long", but the one for column "age" is "string". */
  private static final String CONTROL_DEFAULT_LONG_AGE_STRING =
      "age-default-long-age-string" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "string", but the one for column "age" is "long". */
  private static final String CONTROL_DEFAULT_STRING_AGE_LONG =
      "age-default-string-age-long" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "double", but the one for column "age" is "long". */
  private static final String CONTROL_DEFAULT_DOUBLE_AGE_LONG =
      "age-default-double-age-long" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  /** Column name of column "age" loaded from "age.json". */
  private static final String TABLE_AGE_COLUMN_AGE = "age";
  /** Column name of column "index" loaded from "age.json". */
  private static final String TABLE_AGE_COLUMN_INDEX = "index";
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

    // simulate "cluster initialized", although we do not start our local server. But we need to get the consensus
    // running!
    ConsensusServerTestUtil.configureMemoryOnlyStorage(dataContext.getBean(ConsensusServer.class));
    dataContext.getBeansOfType(ClusterManagerListener.class).values().forEach(l -> l.clusterInitialized());

    tableRegistry = dataContext.getBean(TableRegistry.class);

    controlFileFactory = new Function<File, ControlFileLoader>() {
      @Override
      public ControlFileLoader apply(File controlFile) {
        return new ControlFileLoader( //
            tableRegistry, //
            dataContext.getBean(TableFactory.class), //
            dataContext.getBean(CsvLoader.class), //
            dataContext.getBean(JsonLoader.class), //
            dataContext.getBean(DiqubeLoader.class), //
            dataContext.getBean(ClusterFlattenServiceHandler.class), //
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
    String table = cfl.load().getLeft();

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
    String table = cfl.load().getLeft();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertEquals(shard.getLowestRowId(), 5L, "Expected correct first row");
  }

  @Test
  public void jsonDefaultLongAgeString() throws LoadException {
    // GIVEN
    ControlFileLoader cfl = controlFileFactory.apply(new File(testDir, CONTROL_DEFAULT_LONG_AGE_STRING));

    // WHEN
    String table = cfl.load().getLeft();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertTrue(shard.getStringColumns().containsKey(TABLE_AGE_COLUMN_AGE),
        "Expected column '" + TABLE_AGE_COLUMN_AGE + "' to be of type string.");
    Assert.assertTrue(shard.getLongColumns().containsKey(TABLE_AGE_COLUMN_INDEX),
        "Expected column '" + TABLE_AGE_COLUMN_INDEX + "' to be of type long (=default).");
  }

  @Test
  public void jsonDefaultStringAgeLong() throws LoadException {
    // GIVEN
    ControlFileLoader cfl = controlFileFactory.apply(new File(testDir, CONTROL_DEFAULT_STRING_AGE_LONG));

    // WHEN
    String table = cfl.load().getLeft();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertTrue(shard.getLongColumns().containsKey(TABLE_AGE_COLUMN_AGE),
        "Expected column '" + TABLE_AGE_COLUMN_AGE + "' to be of type long.");
    // As the default is "string", but JsonLoader identifies that Index is "long", we have a more exact match found -
    // use long! If there would be a specific override for column "index" in the control file, that would have to be
    // used, but there is not (only a "default" is specified).
    Assert.assertTrue(shard.getLongColumns().containsKey(TABLE_AGE_COLUMN_INDEX),
        "Expected column '" + TABLE_AGE_COLUMN_INDEX + "' to be of type long.");
  }

  @Test
  public void jsonDefaultDoubleAgeLong() throws LoadException {
    // GIVEN
    ControlFileLoader cfl = controlFileFactory.apply(new File(testDir, CONTROL_DEFAULT_DOUBLE_AGE_LONG));

    // WHEN
    String table = cfl.load().getLeft();

    // THEN
    Assert.assertNotNull(tableRegistry.getTable(table), "Expected loaded table to be available");
    Assert.assertEquals(tableRegistry.getTable(table).getShards().size(), 1, "Correct number of shards expected.");
    TableShard shard = tableRegistry.getTable(table).getShards().iterator().next();
    Assert.assertTrue(shard.getLongColumns().containsKey(TABLE_AGE_COLUMN_AGE),
        "Expected column '" + TABLE_AGE_COLUMN_AGE + "' to be of type long.");
    // As the default is "string", but JsonLoader identifies that Index is "long", we have a more exact match found -
    // use long! If there would be a specific override for column "index" in the control file, that would have to be
    // used, but there is not (only a "default" is specified).
    // TODO #15 introduce import data specificity - when default is "double" but "long" identified, make it "double"
    Assert.assertTrue(shard.getLongColumns().containsKey(TABLE_AGE_COLUMN_INDEX),
        "Expected column '" + TABLE_AGE_COLUMN_INDEX + "' to be of type long.");
  }
}
