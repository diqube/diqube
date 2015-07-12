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
   * All profiles, but not including {@link #NEW_DATA_WATCHER}.
   */
  public static final String[] ALL_BUT_NEW_DATA_WATCHER = new String[] {};

  /**
   * All profiles.
   */
  public static final String[] ALL = new String[] { NEW_DATA_WATCHER };
}
