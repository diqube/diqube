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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.serialize.DeserializationException;
import org.diqube.data.serialize.SerializationException;
import org.diqube.data.table.DefaultTableShard;
import org.diqube.data.table.TableShard;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.file.DiqubeFileReader;
import org.diqube.file.DiqubeFileWriter;
import org.diqube.flatten.FlattenedTableDiskCache;
import org.diqube.listeners.TableLoadListener;
import org.diqube.threads.ExecutorManager;
import org.diqube.util.BigByteBuffer;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;

/**
 * A simple {@link FlattenedTableDiskCache}.
 * 
 * <p>
 * Note that this is a {@link TableLoadListener}. Instantiating classes must make sure to call the methods accordingly!
 *
 * @author Bastian Gloeckle
 */
public class FlattenedControlFileFlattenedTableDiskCache implements FlattenedTableDiskCache, TableLoadListener {
  private static final Logger logger = LoggerFactory.getLogger(FlattenedControlFileFlattenedTableDiskCache.class);

  private static final String FLATTENED_CONTROL_FILE_SUFFIX = ".flattenedcontrol";
  private static final String FLATTENED_DATA_FILE_SUFFIX = ".diqube";

  private static final String FLATTENED_CONTROL_SOURCE_TABLE = "sourceTableName";

  private static final String FLATTENED_CONTROL_FLATTEN_BY = "flattenBy";

  private static final String FLATTENED_CONTROL_ORIG_FIRST_ROW = "origFirstRow";
  private static final String FLATTENED_CONTROL_ORIG_FIRST_ROW_DELIMITER = ",";

  private File cacheDirectory;

  private DiqubeFileFactory diqubeFileFactory;

  private FlattenDataFactory flattenDataFactory;

  private Object sync = new Object();

  /**
   * Holds currently loaded data. Do not use directly, but use {@link #loadCurrentData()}.
   * 
   * <p>
   * Map from table/flattenBy pair to List of {@link CachedDataInfo}.
   * 
   * <p>
   * Sync writing access on {@link #sync}.
   */
  private Map<Pair<String, String>, Deque<CachedDataInfo>> curData = new ConcurrentHashMap<>();

  /**
   * Information on control files. Map from {@link File#getAbsolutePath()} to Pair of last-modified-time and triple of
   * sourceTableName/flattenBy/origFirstRowIds.
   * <p>
   * Sync writing access on {@link #sync}.
   */
  private Map<String, Pair<FileTime, Triple<String, String, Set<Long>>>> controlFileInfo = new ConcurrentHashMap<>();

  private ExecutorService serializationExecutor;

  /* package */ FlattenedControlFileFlattenedTableDiskCache(DiqubeFileFactory diqubeFileFactory,
      FlattenDataFactory flattenDataFactory, ExecutorManager executorManager, File cacheDirectory) {
    this.diqubeFileFactory = diqubeFileFactory;
    this.flattenDataFactory = flattenDataFactory;
    this.cacheDirectory = cacheDirectory;
    serializationExecutor =
        executorManager.newCachedThreadPoolWithMax("flattened-serializer-%d", new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception while serializing a flattened table/putting it into the cache", e);
          }
        }, 1);
  }

  @Override
  protected void finalize() throws Throwable {
    serializationExecutor.shutdownNow();
  }

  @Override
  public void tableLoaded(String tableName) {
    // noop.
  }

  @Override
  public void tableUnloaded(String tableName) {
    List<Pair<String, String>> keysRemoved =
        loadCurrentData().keySet().stream().filter(p -> p.getLeft().equals(tableName)).collect(Collectors.toList());
    logger.info("Removing flattenedcache entries for removed table {}", tableName);
    for (Pair<String, String> keyPair : keysRemoved) {
      Deque<CachedDataInfo> infoDeque = curData.get(keyPair);
      while (infoDeque != null && !infoDeque.isEmpty()) {
        CachedDataInfo info = infoDeque.poll();
        if (info != null)
          removeCacheFiles(info, keyPair.getLeft(), keyPair.getRight());
      }
    }
  }

  @Override
  public FlattenedTable load(String sourceTableName, String flattenBy, Set<Long> originalFirstRowIdsOfShards) {
    Map<Pair<String, String>, Deque<CachedDataInfo>> data = loadCurrentData();

    Pair<String, String> keyPair = new Pair<>(sourceTableName, flattenBy);
    Deque<CachedDataInfo> deque = data.get(keyPair);
    if (deque == null)
      return null;

    for (CachedDataInfo info : deque) {
      if (info.getOrigFirstRowIds().equals(originalFirstRowIdsOfShards)) {
        // Load table!
        logger.info("Found valid flattened table for table '{}' flattened by '{}' in disk cache. Deserializing...",
            sourceTableName, flattenBy);
        try {
          return info.getFlattenedTableSupplier().get();
        } finally {
          logger.info("Flattened table for table '{}' flattened by '{}' loaded from disk cache.", sourceTableName,
              flattenBy);
        }
      }
    }

    return null;
  }

  @Override
  public void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy) {
    offer(flattenedTable, sourceTableName, flattenBy, false);
  }

  /* package */ void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy, boolean sync) {
    Map<Pair<String, String>, Deque<CachedDataInfo>> data = loadCurrentData();

    Pair<String, String> keyPair = new Pair<>(sourceTableName, flattenBy);
    Deque<CachedDataInfo> deque = data.get(keyPair);
    if (deque != null) {
      for (CachedDataInfo info : deque) {
        if (info.getOrigFirstRowIds().equals(flattenedTable.getOriginalFirstRowIdsOfShards())) {
          // we have that one cached already!
          logger.trace("Ignoring offer on flatten table of '{}' by '{}' as we have that one cached already",
              sourceTableName, flattenBy);
          return;
        }
      }
    }

    // Cache it!
    byte[] controlData;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Properties p = new Properties();
      p.setProperty(FLATTENED_CONTROL_SOURCE_TABLE, sourceTableName);
      p.setProperty(FLATTENED_CONTROL_FLATTEN_BY, flattenBy);
      p.setProperty(FLATTENED_CONTROL_ORIG_FIRST_ROW,
          Joiner.on(FLATTENED_CONTROL_ORIG_FIRST_ROW_DELIMITER).join(flattenedTable.getOriginalFirstRowIdsOfShards()));
      p.store(new OutputStreamWriter(baos, Charset.forName("UTF-8")),
          "diqube control file for cache of flattened tables");
      controlData = baos.toByteArray();
    } catch (IOException e) {
      logger.warn("Could not serialize new flattenedcontrol file", e);
      return;
    }

    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      logger.error("SHA-256 not found", e);
      return;
    }
    byte[] digest = messageDigest.digest(controlData);
    String fileNameBase = BaseEncoding.base16().encode(digest).toLowerCase();

    // serialize data and write control file.
    Runnable run = new Runnable() {
      @Override
      public void run() {
        File dataFile = new File(cacheDirectory, fileNameBase + FLATTENED_DATA_FILE_SUFFIX);
        try (FileOutputStream fos = new FileOutputStream(dataFile)) {

          logger.info("Serializing flattened table of table '{}' by '{}' to {}...", sourceTableName, flattenBy,
              dataFile.getAbsolutePath());
          try (DiqubeFileWriter writer = diqubeFileFactory.createDiqubeFileWriter(fos)) {
            writer.setComment("Flattened table '" + sourceTableName + "' by '" + flattenBy + "' with firstRowIds: "
                + flattenedTable.getOriginalFirstRowIdsOfShards().toString());
            for (TableShard shard : flattenedTable.getShards())
              writer.writeTableShard(shard, s -> { /* noop */
              });
          }

          logger.info("Serialized flattened table of table '{}' by '{}' to {}.", sourceTableName, flattenBy,
              dataFile.getAbsolutePath());
        } catch (IOException | SerializationException e) {
          logger.warn("Could not serialize flattened table from '" + sourceTableName + "' by '" + flattenBy + "'");
          return;
        }

        try (FileOutputStream fos =
            new FileOutputStream(new File(cacheDirectory, fileNameBase + FLATTENED_CONTROL_FILE_SUFFIX))) {

          fos.write(controlData);

        } catch (IOException e) {
          logger.warn(
              "Could not write flattenedcontrol file of table from '" + sourceTableName + "' by '" + flattenBy + "'");
          return;
        }

        // delete old entries from cache.
        if (curData.containsKey(keyPair)) {
          for (CachedDataInfo info : curData.get(keyPair)) {
            if (!info.getOrigFirstRowIds().equals(flattenedTable.getOriginalFirstRowIdsOfShards()))
              removeCacheFiles(info, sourceTableName, flattenBy);
          }
        }

        // Note that #curData will be updated automatically on next call to loadCurrentData.
      }
    };

    if (sync)
      run.run();
    else
      serializationExecutor.execute(run);
  }

  private void removeCacheFiles(CachedDataInfo info, String sourceTableName, String flattenBy) {
    logger.info("Removing flattenedcache entry of '{}' by '{}'; control file {}", sourceTableName, flattenBy,
        info.getControlFileName());
    File controlFile = new File(info.getControlFileName());
    controlFile.delete();
    new File(dataFileName(controlFile)).delete();
  }

  // visible for testing
  /* package */ Map<Pair<String, String>, Deque<CachedDataInfo>> loadCurrentData() {
    File[] controlFiles =
        cacheDirectory.listFiles(f -> f.isFile() && f.getName().endsWith(FLATTENED_CONTROL_FILE_SUFFIX));

    // evict data from files that have been removed
    Set<String> removedFiles = Sets.difference(controlFileInfo.keySet(),
        Stream.of(controlFiles).map(f -> f.getAbsolutePath()).collect(Collectors.toSet()));
    if (!removedFiles.isEmpty()) {
      for (String removedFile : removedFiles) {
        synchronized (sync) {
          Pair<FileTime, Triple<String, String, Set<Long>>> p = controlFileInfo.remove(removedFile);
          if (p != null) {
            String tableName = p.getRight().getLeft();
            String flattenBy = p.getRight().getMiddle();
            Set<Long> firstRowIds = p.getRight().getRight();

            logger.info(
                "Identified removal of {} from flattenedcache. Cache will not provide "
                    + "flattened tables anymore on following values: {}/{}/{} (last limit)",
                removedFile, tableName, flattenBy, Iterables.limit(firstRowIds, 100));

            Deque<CachedDataInfo> deque = curData.remove(new Pair<>(tableName, flattenBy));
            Iterator<CachedDataInfo> it = deque.iterator();
            while (it.hasNext()) {
              CachedDataInfo cur = it.next();
              if (cur.getOrigFirstRowIds().equals(firstRowIds))
                it.remove();
            }
          }
        }
      }
    }

    for (File controlFile : controlFiles) {
      FileTime modifiedTime = modifiedTime(controlFile);
      if (modifiedTime == null)
        continue;

      // check if file is new or changed.
      if (!controlFileInfo.containsKey(controlFile.getAbsolutePath())
          || !controlFileInfo.get(controlFile.getAbsolutePath()).getLeft().equals(modifiedTime)) {
        File dataFile = new File(dataFileName(controlFile));

        if (!dataFile.exists() || !dataFile.isFile()) {
          logger.warn("Data file for cached flattened table '{}' does not exist or is directory. Ignoring.",
              dataFile.getAbsolutePath());
          continue;
        }

        synchronized (sync) {
          // re-check if file changed in the meantime.
          modifiedTime = modifiedTime(controlFile);
          if (modifiedTime == null)
            continue;

          if (!controlFileInfo.containsKey(controlFile.getAbsolutePath())
              || !controlFileInfo.get(controlFile.getAbsolutePath()).getLeft().equals(modifiedTime)) {

            Properties control = new Properties();
            try (FileInputStream fis = new FileInputStream(controlFile)) {
              control.load(new InputStreamReader(fis, Charset.forName("UTF-8")));
            } catch (IOException e) {
              logger.warn("IOException while trying to access control file in flattenedcache: {}. Ignoring.",
                  controlFile.getAbsolutePath(), e);
              continue;
            }

            String sourceTableName = control.getProperty(FLATTENED_CONTROL_SOURCE_TABLE);
            String flattenBy = control.getProperty(FLATTENED_CONTROL_FLATTEN_BY);
            String origFirstRow = control.getProperty(FLATTENED_CONTROL_ORIG_FIRST_ROW);

            if (sourceTableName == null || flattenBy == null || origFirstRow == null) {
              logger.warn("Control file of flattenedcache is invalid: {}. Ignoring.", controlFile.getAbsolutePath());
              continue;
            }

            String[] firstRowIds = origFirstRow.split(Pattern.quote(FLATTENED_CONTROL_ORIG_FIRST_ROW_DELIMITER));
            Set<Long> firstRowIdsSet = new HashSet<>();
            boolean error = false;
            for (String firstRowIdString : firstRowIds) {
              try {
                firstRowIdsSet.add(Long.parseLong(firstRowIdString));
              } catch (NumberFormatException e) {
                logger.warn("Control file of flattenedcache is invalid: {}. Ignoring.", controlFile.getAbsolutePath(),
                    e);
                error = true;
                break;
              }
            }
            if (error)
              continue;

            Supplier<FlattenedTable> loader = new Supplier<FlattenedTable>() {
              @Override
              public FlattenedTable get() {
                try (RandomAccessFile f = new RandomAccessFile(dataFile, "r")) {
                  BigByteBuffer buf = new BigByteBuffer(f.getChannel(), MapMode.READ_ONLY, b -> b.load());

                  DiqubeFileReader fileReader = diqubeFileFactory.createDiqubeFileReader(buf);

                  Collection<DefaultTableShard> shards = fileReader.loadAllTableShards();

                  return flattenDataFactory.createFlattenedTable(
                      "FLATTENED_LOADED" /* No need to guarantee a specific table name */,
                      shards.stream().map(s -> (TableShard) s).collect(Collectors.toList()), firstRowIdsSet);
                } catch (IOException | DeserializationException e) {
                  logger.error("Could not load disk-cached flattened table from {}", dataFile.getAbsolutePath(), e);
                  return null;
                }
              }
            };

            logger.info(
                "Found new/changed flattenedcache control file '{}'. Cache will provide data on following "
                    + "values in the future: {}/{}/{} (last limit)",
                controlFile.getAbsolutePath(), sourceTableName, flattenBy, Iterables.limit(firstRowIdsSet, 100));

            Pair<String, String> keyPair = new Pair<>(sourceTableName, flattenBy);
            curData.computeIfAbsent(keyPair, k -> new ConcurrentLinkedDeque<CachedDataInfo>());
            curData.get(keyPair).add(new CachedDataInfo(firstRowIdsSet, controlFile.getAbsolutePath(), loader));

            controlFileInfo.put(controlFile.getAbsolutePath(),
                new Pair<>(modifiedTime, new Triple<>(sourceTableName, flattenBy, firstRowIdsSet)));
          }
        }
      }
    }

    return curData;
  }

  private String dataFileName(File controlFile) {
    String control = controlFile.getAbsolutePath();
    return control.substring(0, control.length() - FLATTENED_CONTROL_FILE_SUFFIX.length()) + FLATTENED_DATA_FILE_SUFFIX;
  }

  private FileTime modifiedTime(File f) {
    try {
      return Files.getLastModifiedTime(f.toPath());
    } catch (IOException e) {
      logger.warn("IOException while trying to access control file in flattenedcache: {}. Ignoring.",
          f.getAbsolutePath(), e);
      return null;
    }
  }

  /* package */ static class CachedDataInfo {
    private Set<Long> origFirstRowIds;
    private String controlFileName;
    private Supplier<FlattenedTable> flattenedTableSupplier;

    CachedDataInfo(Set<Long> origFirstRowIds, String controlFileName, Supplier<FlattenedTable> flattenedTableSupplier) {
      this.origFirstRowIds = origFirstRowIds;
      this.controlFileName = controlFileName;
      this.flattenedTableSupplier = flattenedTableSupplier;
    }

    public Set<Long> getOrigFirstRowIds() {
      return origFirstRowIds;
    }

    public String getControlFileName() {
      return controlFileName;
    }

    public Supplier<FlattenedTable> getFlattenedTableSupplier() {
      return flattenedTableSupplier;
    }
  }

}
