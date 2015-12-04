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
package org.diqube.config;

/**
 * Keys for "derived config values", that is config values whose final value depends on the values of {@link ConfigKey}
 * keys, but are calculated.
 *
 * @author Bastian Gloeckle
 */
public class DerivedConfigKey {
  /**
   * @see ConfigKey#DATA_DIR
   */
  public static final String FINAL_DATA_DIR = "derived.finalDataDir";

  /**
   * @see ConfigKey#CONSENSUS_DATA_DIR
   */
  public static final String FINAL_CONSENSUS_DATA_DIR = "derived.finalConsensusDataDir";

  /**
   * @see ConfigKey#FLATTEN_DISK_CACHE_LOCATION
   */
  public static final String FINAL_FLATTEN_DISK_CACHE_LOCATION = "derived.finalFlattenDiskCacheLocation";

  /**
   * @see ConfigKey#INTERNAL_DB_DIR
   */
  public static final String FINAL_INTERNAL_DB_DIR = "derived.finalInternalDbDir";
}
