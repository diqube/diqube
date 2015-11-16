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
package org.diqube.flatten;

import java.util.Set;

import org.diqube.context.AutoInstatiate;
import org.diqube.context.Profiles;
import org.diqube.data.flatten.FlattenedTable;
import org.springframework.context.annotation.Profile;

/**
 * A noop implementation of {@link FlattenedTableDiskCache} which is only used in tests (see
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
@Profile(Profiles.TEST_NOOP_FLATTENED_DISK_CACHE)
public class NoopFlattenedTableDiskCache implements FlattenedTableDiskCache {

  @Override
  public FlattenedTable load(String sourceTableName, String flattenBy, Set<Long> originalFirstRowIdsOfShards) {
    return null;
  }

  @Override
  public void offer(FlattenedTable flattenedTable, String sourceTableName, String flattenBy) {
  }

}
