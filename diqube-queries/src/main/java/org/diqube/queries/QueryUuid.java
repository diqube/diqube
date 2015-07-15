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
package org.diqube.queries;

import java.util.UUID;

import org.diqube.util.Pair;

/**
 * Class that can provide the query UUID and execution UUID to any thread while executing a query.
 * 
 * A query UUID is and ID identifying a global query across the whole diqube cluster - meaning the execution of one diql
 * string that was sent by the user. A query can have multiple executions, though - namely one for the query master and
 * then for each query remote another one.
 *
 * @author Bastian Gloeckle
 */
public class QueryUuid {
  private static final ThreadLocal<Pair<UUID, UUID>> queryUuidThreadLocal = new ThreadLocal<>();

  /**
   * @param queryUuid
   *          The query UUID for the current thread.
   * @param executionUuid
   *          The execution UUID for the current thread.
   */
  public static void setCurrentQueryUuidAndExecutionUuid(UUID queryUuid, UUID executionUuid) {
    queryUuidThreadLocal.set(new Pair<>(queryUuid, executionUuid));
  }

  /**
   * Call this method to clear the resources taken up from a call to {@link #setCurrentQueryUuidAndExecutionUuid(UUID)}.
   */
  public static void clearCurrent() {
    queryUuidThreadLocal.set(null);
  }

  /**
   * @return The query UUID of the current thread if available, else <code>null</code>. The returned UUID is that query
   *         UUID that the current thread is supposed to work for.
   */
  public static UUID getCurrentQueryUuid() {
    Pair<UUID, UUID> p = queryUuidThreadLocal.get();
    if (p == null)
      return null;
    return p.getLeft();
  }

  /**
   * @return The execution UUID of the current thread if available, else <code>null</code>.
   */
  public static UUID getCurrentExecutionUuid() {
    Pair<UUID, UUID> p = queryUuidThreadLocal.get();
    if (p == null)
      return null;
    return p.getRight();
  }

}
