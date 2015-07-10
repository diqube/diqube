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
package org.diqube.execution;

import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.env.DelegatingExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.ExecutionEnvironmentFactory;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link ColumnVersionManager} handles creation of intermediate {@link VersionedExecutionEnvironment} which hold
 * different versions of columns during execution of a {@link ExecutablePlan} on the query master.
 * 
 * <p>
 * Using this manager ensures that {@link VersionedExecutionEnvironment} with a higher version has the same or more
 * information available than smaller versions. It will never happen that a {@link VersionedExecutionEnvironment} with
 * higher version number has less information than one with lower version.
 * 
 * <p>
 * "More information" hereby means, that the newer {@link VersionedExecutionEnvironment} has information about more
 * columns, more rows in a column etc.
 *
 * @author Bastian Gloeckle
 */
public class ColumnVersionManager {
  private static final Logger logger = LoggerFactory.getLogger(ColumnVersionManager.class);

  private ExecutionEnvironmentFactory executionEnvironmentFactory;
  private ExecutionEnvironment defaultEnv;
  private VersionedExecutionEnvironment lastVersionedExecutionEnvironment = null;

  public ColumnVersionManager(ExecutionEnvironmentFactory executionEnvironmentFactory, ExecutionEnvironment defaultEnv) {
    this.executionEnvironmentFactory = executionEnvironmentFactory;
    this.defaultEnv = defaultEnv;
  }

  /**
   * Create a new version of an intermediary {@link ExecutionEnvironment} with the given updated columns.
   * 
   * @param updatedColShard
   *          The columns that have new values. These columns will override the values the previous intermediary
   *          {@link ExecutionEnvironment} had of those columns.
   * @return a new {@link VersionedExecutionEnvironment} that has strictly more data than the previous one. The returned
   *         {@link VersionedExecutionEnvironment} will be unmodifiable and calls to the store* methods will throw a
   *         corresponding exception.
   */
  public synchronized VersionedExecutionEnvironment createNewVersion(ColumnShard... updatedColShard) {
    DelegatingExecutionEnvironment res;
    if (lastVersionedExecutionEnvironment == null)
      res = executionEnvironmentFactory.createDelegatingExecutionEnvironment(defaultEnv, Integer.MIN_VALUE);
    else
      res =
          executionEnvironmentFactory.createDelegatingExecutionEnvironment(lastVersionedExecutionEnvironment,
              lastVersionedExecutionEnvironment.getVersion() + 1);

    if (updatedColShard != null) {
      for (ColumnShard newShard : updatedColShard) {
        logger.trace("New ExecutionEnvironment, version {}, updated col {}", res.getVersion(), newShard.getName());
        switch (newShard.getColumnType()) {
        case STRING:
          res.storeTemporaryStringColumnShard((StringColumnShard) newShard);
          break;
        case LONG:
          res.storeTemporaryLongColumnShard((LongColumnShard) newShard);
          break;
        case DOUBLE:
          res.storeTemporaryDoubleColumnShard((DoubleColumnShard) newShard);
          break;
        }
      }
    }

    res.makeUnmodifiable();

    lastVersionedExecutionEnvironment = res;

    return res;
  }
}
