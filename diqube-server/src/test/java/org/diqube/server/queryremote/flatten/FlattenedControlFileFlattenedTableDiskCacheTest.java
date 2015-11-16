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
package org.diqube.server.queryremote.flatten;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.diqube.context.Profiles;
import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileReader;
import org.diqube.file.DiqubeFileWriter;
import org.diqube.server.queryremote.flatten.FlattenedControlFileFlattenedTableDiskCache.CachedDataInfo;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link FlattenedControlFileFlattenedTableDiskCache}.
 *
 * @author Bastian Gloeckle
 */
public class FlattenedControlFileFlattenedTableDiskCacheTest {
  private static final String TABLE = "table";
  private static final String FLATTEN_BY = "a[*]";

  private AnnotationConfigApplicationContext dataContext;
  private Path cacheDir;
  private FlattenedControlFileFlattenedTableDiskCache diskCache;
  private Holder<DiqubeFileWriter> writerHolder;
  private Holder<DiqubeFileReader> readerHolder;

  @BeforeMethod
  public void before() throws IOException {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    cacheDir = Files.createTempDirectory(FlattenedControlFileFlattenedTableDiskCacheTest.class.getSimpleName());

    writerHolder = new Holder<>();
    readerHolder = new Holder<>();

    DiqubeFileFactory fileFactory = Mockito.mock(DiqubeFileFactory.class);

    Mockito.when(fileFactory.createDiqubeFileWriter(Mockito.any())).thenAnswer(invocation -> {
      DiqubeFileWriter res = Mockito.mock(DiqubeFileWriter.class);
      writerHolder.setValue(res);
      return res;
    });
    Mockito.when(fileFactory.createDiqubeFileReader(Mockito.any())).thenAnswer(invocation -> {
      DiqubeFileReader res = Mockito.mock(DiqubeFileReader.class);
      readerHolder.setValue(res);
      return res;
    });

    diskCache = new FlattenedControlFileFlattenedTableDiskCache(fileFactory,
        dataContext.getBean(FlattenDataFactory.class), dataContext.getBean(ExecutorManager.class), cacheDir.toFile());
  }

  @AfterMethod
  public void after() throws Throwable {
    diskCache.finalize();
    dataContext.close();
    Files.walkFileTree(cacheDir, new FileVisitor<Path>() {
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
    cacheDir.toFile().delete();
  }

  @Test
  public void offerAddsTable() {
    // GIVEN
    FlattenedTable flattenedTable = mockedFlattenedTable(0L, 10L);

    // WHEN
    diskCache.offer(flattenedTable, TABLE, FLATTEN_BY, true);

    // THEN
    Assert.assertEquals(diskCache.loadCurrentData().keySet(),
        new HashSet<>(Arrays.asList(new Pair<>(TABLE, FLATTEN_BY))), "Expected correct table/flattenBy pair is cached");
  }

  @Test
  public void tableRemoveRemovesCache() {
    // GIVEN
    FlattenedTable flattenedTable = mockedFlattenedTable(0L, 10L);

    // WHEN
    diskCache.offer(flattenedTable, TABLE, FLATTEN_BY, true);
    diskCache.tableUnloaded(TABLE);

    // THEN
    Assert.assertEquals(diskCache.loadCurrentData().keySet(), new HashSet<>(),
        "Expected correct table/flattenBy pair is cached");
  }

  @Test
  public void oldVersionsAreRemoved() {
    // GIVEN
    FlattenedTable flattenedTable = mockedFlattenedTable(0L, 10L);
    FlattenedTable flattenedTableNewVersion = mockedFlattenedTable(0L);

    // WHEN
    diskCache.offer(flattenedTable, TABLE, FLATTEN_BY, true);
    diskCache.offer(flattenedTableNewVersion, TABLE, FLATTEN_BY, true);

    // THEN
    Pair<String, String> keyPair = new Pair<>(TABLE, FLATTEN_BY);
    Map<Pair<String, String>, Deque<CachedDataInfo>> data = diskCache.loadCurrentData();
    Assert.assertEquals(data.keySet(), new HashSet<>(Arrays.asList(keyPair)),
        "Expected correct table/flattenBy pair is cached");
    Set<Set<Long>> expectedRowIdSet = new HashSet<>();
    expectedRowIdSet.add(new HashSet<>(Arrays.asList(0L)));
    Assert.assertEquals(data.get(keyPair).stream().map(info -> info.getOrigFirstRowIds()).collect(Collectors.toSet()),
        expectedRowIdSet, "Expected correct table/flattenBy pair is cached");
  }

  private FlattenedTable mockedFlattenedTable(Long... firstRowIds) {
    FlattenedTable res = Mockito.mock(FlattenedTable.class);
    Mockito.when(res.getOriginalFirstRowIdsOfShards()).thenReturn(new HashSet<>(Arrays.asList(firstRowIds)));
    return res;
  }

}
