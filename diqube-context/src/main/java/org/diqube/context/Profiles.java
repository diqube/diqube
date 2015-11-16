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
package org.diqube.context;

import org.springframework.context.annotation.Profile;

/**
 * Profiles can be used with the annotation {@link Profile} on beans to enable/disable a specific "feature" (=creation
 * of a bean).
 *
 * @author Bastian Gloeckle
 */
public class Profiles {
  /**
   * Enable the NewDataWatcher in the server which will watch a directory for new data to be loaded. JUnit tests might
   * want to disable this, if NewDataLoader class is on their classpath and would be loaded by the context.
   */
  public static final String NEW_DATA_WATCHER = "NewDataWatcher";

  /**
   * Enable configuration system.
   */
  public static final String CONFIG = "Config";

  /**
   * All profiles, but not including {@link #NEW_DATA_WATCHER}.
   */
  public static final String[] ALL_BUT_NEW_DATA_WATCHER = new String[] { CONFIG };

  /**
   * All profiles.
   */
  public static final String[] ALL = new String[] { NEW_DATA_WATCHER, CONFIG };

  /**
   * Profile which will instantiate a noop implementation of FlattenedTableDiskCache. Not included in any of the other
   * profiles, must not be used when the classpath contains an actual implementation of FLattenedTableDiskCache.
   */
  public static final String TEST_NOOP_FLATTENED_DISK_CACHE = "TestNoopFlattenedDiskCache";

  /**
   * All profiles, but not including {@link #NEW_DATA_WATCHER}.
   */
  public static final String[] TEST_ALL_BUT_NEW_DATA_WATCHER;

  static {
    TEST_ALL_BUT_NEW_DATA_WATCHER = new String[ALL_BUT_NEW_DATA_WATCHER.length + 1];
    for (int i = 0; i < ALL_BUT_NEW_DATA_WATCHER.length; i++)
      TEST_ALL_BUT_NEW_DATA_WATCHER[i] = ALL_BUT_NEW_DATA_WATCHER[i];
    TEST_ALL_BUT_NEW_DATA_WATCHER[TEST_ALL_BUT_NEW_DATA_WATCHER.length - 1] = TEST_NOOP_FLATTENED_DISK_CACHE;
  }
}
