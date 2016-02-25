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

import java.io.File;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.diqube.config.Config;
import org.diqube.config.DerivedConfigKey;
import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.data.flatten.FlattenDataFactory;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.file.DiqubeFileFactory;
import org.diqube.flatten.FlattenedTableDiskCache;
import org.diqube.listeners.TableLoadListener;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;

/**
 * A {@link FlattenedTableDiskCache} that delegates to different implementations of {@link FlattenedTableDiskCache}
 * based on the server configuration.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.FLATTEN_DISK_CACHE)
public class DelegatingFlattenedTableDiskCache implements FlattenedTableDiskCache, TableLoadListener {
  private static final Logger logger = LoggerFactory.getLogger(DelegatingFlattenedTableDiskCache.class);

  private FlattenedTableDiskCache delegate;

  @Inject
  private DiqubeFileFactory diqubeFileFactory;

  @Inject
  private FlattenDataFactory flattenDataFactory;

  @Inject
  private ExecutorManager executorManager;

  @Config(DerivedConfigKey.FINAL_FLATTEN_DISK_CACHE_LOCATION)
  private String cacheLocation;

  @PostConstruct
  public void initialize() {
    if (cacheLocation == null || "none".equals(cacheLocation)) {
      logger.info("Disabling flattened table disk cache.");
      delegate = new NoopFlattenedTableDiskCache();
    } else {
      File cacheLocationFile = new File(cacheLocation);

      if (cacheLocationFile.exists() && !cacheLocationFile.isDirectory())
        throw new RuntimeException(
            "Flatten disk cache directory '" + cacheLocationFile.getAbsolutePath() + "' exists but is no directory.");

      if (!cacheLocationFile.exists() && !cacheLocationFile.mkdirs())
        throw new RuntimeException(
            "Could not create flatten disk cache directory '" + cacheLocationFile.getAbsolutePath() + "'.");

      logger.info("Using '{}' as flattened table disk cache directory.", cacheLocationFile.getAbsolutePath());

      delegate = new FlattenedControlFileFlattenedTableDiskCache(diqubeFileFactory, flattenDataFactory, executorManager,
          cacheLocationFile);
    }
  }

  @Override
  public FlattenedTable load(String sourceTableName, String flattenBy, Set<Long> originalFirstRowIdsOfShards) {
    return delegate.load(sourceTableName, flattenBy, originalFirstRowIdsOfShards);
  }

  @Override
  public void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy) {
    delegate.offer(flattenedTable, sourceTableName, flattenBy);
  }

  @Override
  public void tableLoaded(String tableName) throws AbortTableLoadException {
    if (delegate instanceof TableLoadListener)
      ((TableLoadListener) delegate).tableLoaded(tableName);
  }

  @Override
  public void tableUnloaded(String tableName) {
    if (delegate instanceof TableLoadListener)
      ((TableLoadListener) delegate).tableUnloaded(tableName);
  }

  /**
   * No Operation implementation of {@link FlattenedTableDiskCache}.
   */
  private static final class NoopFlattenedTableDiskCache implements FlattenedTableDiskCache {
    @Override
    public FlattenedTable load(String sourceTableName, String flattenBy, Set<Long> originalFirstRowIdsOfShards) {
      // noop.
      return null;
    }

    @Override
    public void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy) {
      // noop.
    }

  }

}
