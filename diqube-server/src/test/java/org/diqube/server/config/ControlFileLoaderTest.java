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
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;

import org.diqube.context.Profiles;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.executionenv.TableRegistry;
import org.diqube.loader.CsvLoader;
import org.diqube.loader.DiqubeLoader;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.metadata.TableMetadataManager;
import org.diqube.metadata.create.TableShardMetadataBuilderFactory;
import org.diqube.server.ControlFileManager;
import org.diqube.server.control.ControlFileLoader;
import org.diqube.server.metadata.ServerTableMetadataPublisher;
import org.diqube.server.metadata.ServerTableMetadataPublisherTestUtil;
import org.diqube.server.queryremote.flatten.ClusterFlattenServiceHandler;
import org.diqube.thrift.base.thrift.FieldMetadata;
import org.diqube.thrift.base.thrift.FieldType;
import org.diqube.thrift.base.thrift.TableMetadata;
import org.diqube.util.Pair;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.mockito.Mockito;
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
  private static final String CONTROL_AGE_FIRSTROW0 = "age-firstrow0" + ControlFileManager.CONTROL_FILE_EXTENSION;
  /** A test .control-file with JSON & firstRowId = 5 */
  private static final String CONTROL_AGE_FIRSTROW5 = "age-firstrow5" + ControlFileManager.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "long", but the one for column "age" is "string". */
  private static final String CONTROL_DEFAULT_LONG_AGE_STRING =
      "age-default-long-age-string" + ControlFileManager.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "string", but the one for column "age" is "long". */
  private static final String CONTROL_DEFAULT_STRING_AGE_LONG =
      "age-default-string-age-long" + ControlFileManager.CONTROL_FILE_EXTENSION;
  /** Control file where the default column type is "double", but the one for column "age" is "long". */
  private static final String CONTROL_DEFAULT_DOUBLE_AGE_LONG =
      "age-default-double-age-long" + ControlFileManager.CONTROL_FILE_EXTENSION;
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

  private TableMetadataManager metadataManagerMock;

  @BeforeMethod
  public void setup() throws IOException {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.UNIT_TEST);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    tableRegistry = dataContext.getBean(TableRegistry.class);
    metadataManagerMock = Mockito.mock(TableMetadataManager.class);

    ServerTableMetadataPublisher metadataPublisher =
        ServerTableMetadataPublisherTestUtil.create(dataContext.getBean(TableRegistry.class),
            dataContext.getBean(TableShardMetadataBuilderFactory.class), metadataManagerMock);

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
            metadataPublisher, //
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

    assertMetadataPublished(table, new Pair<>(TABLE_AGE_COLUMN_AGE, FieldType.LONG),
        new Pair<>(TABLE_AGE_COLUMN_INDEX, FieldType.LONG));
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

    assertMetadataPublished(table, new Pair<>(TABLE_AGE_COLUMN_AGE, FieldType.LONG),
        new Pair<>(TABLE_AGE_COLUMN_INDEX, FieldType.LONG));
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

    assertMetadataPublished(table, new Pair<>(TABLE_AGE_COLUMN_AGE, FieldType.STRING),
        new Pair<>(TABLE_AGE_COLUMN_INDEX, FieldType.LONG));
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

    assertMetadataPublished(table, new Pair<>(TABLE_AGE_COLUMN_AGE, FieldType.LONG),
        new Pair<>(TABLE_AGE_COLUMN_INDEX, FieldType.LONG));
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

    assertMetadataPublished(table, new Pair<>(TABLE_AGE_COLUMN_AGE, FieldType.LONG),
        new Pair<>(TABLE_AGE_COLUMN_INDEX, FieldType.LONG));
  }

  /**
   * Asserts that correct {@link TableMetadata} has been published.
   * 
   * @param tableName
   *          Name of the table expected in {@link TableMetadata}.
   * @param fields
   *          The fields that are required in the {@link TableMetadata}. Pair of field name and field type.
   */
  @SafeVarargs
  private final void assertMetadataPublished(String tableName, Pair<String, FieldType>... fields) {
    Mockito.verify(metadataManagerMock).adjustTableMetadata(Mockito.anyString(),
        Mockito.argThat(new BaseMatcher<Function<TableMetadata, TableMetadata>>() {
          @Override
          public boolean matches(Object item) {
            if (!(item instanceof Function))
              return false;

            @SuppressWarnings("unchecked")
            Function<TableMetadata, TableMetadata> fn = (Function<TableMetadata, TableMetadata>) item;

            // assert correct metadata if FN receives null
            TableMetadata m = fn.apply(null);

            Assert.assertEquals(m.getTableName(), tableName, "Table name of table metadata should be correct.");
            for (Pair<String, FieldType> f : fields) {
              Optional<FieldMetadata> metatdata =
                  m.getFields().stream().filter(fm -> fm.getFieldName().equals(f.getLeft())).findAny();
              Assert.assertTrue(metatdata.isPresent(), "Field " + f + " should be available");
              Assert.assertEquals(metatdata.get().getFieldType(), f.getRight(),
                  "Field type of field " + f + " should be correct.");
            }

            // assert correct metadata if FN receives another (in this case empty) table metadata
            m = fn.apply(new TableMetadata(tableName, new ArrayList<>()));

            Assert.assertEquals(m.getTableName(), tableName, "Table name of table metadata should be correct.");
            for (Pair<String, FieldType> f : fields) {
              Optional<FieldMetadata> metatdata =
                  m.getFields().stream().filter(fm -> fm.getFieldName().equals(f.getLeft())).findAny();
              Assert.assertTrue(metatdata.isPresent(), "Field " + f + " should be available");
              Assert.assertEquals(metatdata.get().getFieldType(), f.getRight(),
                  "Field type of field " + f + " should be correct.");
            }

            return true;
          }

          @Override
          public void describeTo(Description description) {
            description.appendText("Correct TableMetadata is produced");
          }
        }));
  }
}
